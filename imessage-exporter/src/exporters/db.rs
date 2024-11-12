use std::{ borrow::Cow, collections::HashMap, fs::File, io::{ BufWriter, Write }, path::PathBuf };

use crate::{
    app::{
        attachment_manager::AttachmentManager,
        error::RuntimeError,
        progress::build_progress_bar_export,
        runtime::Config,
    },
    exporters::exporter::Exporter,
};

use imessage_database::{
    error::{ plist::PlistParseError, table::TableError },
    message_types::variants::Variant,
    tables::{
        attachment::Attachment,
        messages::{ models::BubbleComponent, Message },
        table::{ Table, FITNESS_RECEIVER, ME, YOU },
    },
    util::dates::format_utc,
};

use imessage_database::{
    message_types::{
        app::AppMessage,
        app_store::AppStoreMessage,
        collaboration::CollaborationMessage,
        edited::{ EditStatus, EditedMessage },
        expressives::{ BubbleEffect, Expressive, ScreenEffect },
        handwriting::HandwrittenMessage,
        music::MusicMessage,
        placemark::PlacemarkMessage,
        text_effects::TextEffect,
        url::URLMessage,
        variants::{ Announcement, BalloonProvider, CustomBalloon, URLOverride },
    },
    util::{
        dates::{ format, get_local_time, readable_diff, TIMESTAMP_FACTOR, get_utc_time },
        plist::parse_plist,
    },
};
use super::exporter::{ BalloonFormatter, Writer };
use lib_db::{ Database, DatabaseType };

pub struct DB<'a> {
    /// Data that is setup from the application's runtime
    pub config: &'a Config,

    /// Buffer to store messages before batch insert
    pub messages: Vec<lib_db::Message>,

    /// Database exporter
    pub database: Option<Box<dyn Database>>,

    /// Log file writer
    pub log_writer: Option<BufWriter<File>>,
}

impl<'a> Exporter<'a> for DB<'a> {
    fn new(config: &'a Config) -> Result<Self, RuntimeError> {
        let database = <dyn Database>
            ::new(DatabaseType::Surreal)
            .map_err(|e| RuntimeError::ExportError(e))?;

        Ok(DB {
            config,
            messages: Vec::new(),
            database: Some(database),
            log_writer: None,
        })
    }

    fn iter_messages(&mut self) -> Result<(), RuntimeError> {
        eprintln!("Exporting to database...");

        let mut current_message = 0;
        let total_messages = Message::get_count(
            &self.config.db,
            &self.config.options.query_context
        ).map_err(RuntimeError::DatabaseError)?;
        let pb = build_progress_bar_export(total_messages);

        let mut statement = Message::stream_rows(
            &self.config.db,
            &self.config.options.query_context
        ).map_err(RuntimeError::DatabaseError)?;

        let messages = statement
            .query_map([], |row| Ok(Message::from_row(row)))
            .map_err(|err| RuntimeError::DatabaseError(TableError::Messages(err)))?;

        for message in messages {
            let mut msg = Message::extract(message).map_err(RuntimeError::DatabaseError)?;
            let _ = msg.generate_text(&self.config.db);
            self.write_message(&msg)?;

            current_message += 1;
            if current_message % 99 == 0 {
                pb.set_position(current_message);
            }
        }
        pb.finish();
        // Here we would insert the buffered messages into the database
        self.flush_messages()?;

        // Create graph relations after all messages are exported
        if let Some(db) = &self.database {
            eprintln!("Creating graph relations...");
            let start = std::time::Instant::now();
            let spinner = indicatif::ProgressBar::new_spinner();
            let spinner_clone = spinner.clone();
            spinner.set_message("Creating graph relations");

            let update_display = std::thread::spawn(move || {
                while !spinner_clone.is_finished() {
                    let elapsed = start.elapsed();
                    spinner_clone.set_message(format!("Creating graph relations ({:?})", elapsed));
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            });

            let _ = db.create_graph().map_err(|e| RuntimeError::ExportError(e))?;
            spinner.finish_and_clear();
            update_display.join().unwrap();

            eprintln!("Total time: {:?}", start.elapsed());
        }

        Ok(())
    }

    fn get_or_create_file(
        &mut self,
        _message: &Message
    ) -> Result<&mut BufWriter<File>, RuntimeError> {
        // Create or get the log file writer
        if self.log_writer.is_none() {
            let log_path = self.config.options.export_path.join("db_export.log");
            let file = File::options()
                .write(true)
                .create(true)
                .append(true)
                .open(&log_path)
                .map_err(|err| RuntimeError::CreateError(err, log_path))?;

            self.log_writer = Some(BufWriter::new(file));
        }

        // Safe to unwrap since we just ensured it exists
        Ok(self.log_writer.as_mut().unwrap())
    }
}

impl<'a> DB<'a> {
    fn write_message(&mut self, message: &Message) -> Result<(), RuntimeError> {
        let deduped_chat_id = match self.config.conversation(message) {
            Some((_, id)) => Some(*id),
            None => message.chat_id,
        };

        // Handle unique_chat_id with fallback
        let unique_chat_id = match deduped_chat_id {
            Some(id) => id.to_string(),
            None => {
                // Create fallback string using phone_number
                let phone_number = self.config
                    .who(message.handle_id, message.is_from_me, &message.destination_caller_id)
                    .to_string();
                format!("{}:Missing_chat_id", phone_number)
            }
        };

        let thread_name = self.config
            .conversation(message)
            .map(|(chatroom, _)| self.config.filename(chatroom));

        // Format dates in UTC
        let date = format_utc(&get_utc_time(&message.date, &self.config.offset));
        let date_read = format_utc(&get_utc_time(&message.date_read, &self.config.offset));
        let date_delivered = format_utc(
            &get_utc_time(&message.date_delivered, &self.config.offset)
        );
        let date_edited = format_utc(&get_utc_time(&message.date_edited, &self.config.offset));

        let full_message = self
            .format_message(message, 0)
            .map_err(|e| RuntimeError::DatabaseError(e))?;

        let mut attachment_paths = Vec::new();
        if message.num_attachments > 0 {
            if let Ok(mut attachments) = Attachment::from_message(&self.config.db, message) {
                for attachment in attachments.iter_mut() {
                    if let Ok(path) = self.format_attachment(attachment, message) {
                        attachment_paths.push(path);
                    }
                }
            }
        }

        // Create a Message struct for database insertion
        let db_message = lib_db::Message {
            id: None,
            rowid: message.rowid,
            guid: message.guid.clone(),
            text: message.text.clone(),
            service: message.service.clone(),
            handle_id: message.handle_id,
            destination_caller_id: message.destination_caller_id.clone(),
            subject: message.subject.clone(),
            date,
            date_read,
            date_delivered,
            is_from_me: message.is_from_me,
            is_read: message.is_read,
            item_type: message.item_type,
            other_handle: message.other_handle,
            share_status: message.share_status,
            share_direction: message.share_direction,
            group_title: message.group_title.clone(),
            group_action_type: message.group_action_type,
            associated_message_guid: message.associated_message_guid.clone(),
            associated_message_type: message.associated_message_type,
            balloon_bundle_id: message.balloon_bundle_id.clone(),
            expressive_send_style_id: message.expressive_send_style_id.clone(),
            thread_originator_guid: message.thread_originator_guid.clone(),
            thread_originator_part: message.thread_originator_part.clone(),
            date_edited,
            associated_message_emoji: message.associated_message_emoji.clone(),
            chat_id: message.chat_id,
            unique_chat_id,
            num_attachments: message.num_attachments,
            deleted_from: message.deleted_from,
            num_replies: message.num_replies,
            full_message,
            thread_name,
            attachment_paths,
            is_deleted: message.is_deleted(),
            is_edited: message.is_edited(),
            is_reply: message.is_reply(),
            phone_number: self.config
                .who(message.handle_id, message.is_from_me, &message.destination_caller_id)
                .to_string(),
        };

        self.messages.push(db_message);

        // Flush messages to database if buffer gets too large
        if self.messages.len() >= 1000 {
            self.flush_messages()?;
        }

        Ok(())
    }

    fn get_time(&self, message: &Message) -> String {
        let mut date = format(&message.date(&self.config.offset));
        let read_after = message.time_until_read(&self.config.offset);
        if let Some(time) = read_after {
            if !time.is_empty() {
                let who = if message.is_from_me() {
                    "them"
                } else {
                    self.config.options.custom_name.as_deref().unwrap_or("you")
                };
                date.push_str(&format!(" (Read by {who} after {time})"));
            }
        }
        date
    }
    fn add_line(&self, string: &mut String, part: &str, indent: &str) {
        if !part.is_empty() {
            string.push_str(indent);
            string.push_str(part);
            string.push('\n');
        }
    }
    fn flush_messages(&mut self) -> Result<(), RuntimeError> {
        if let Some(db) = &self.database {
            let messages = std::mem::take(&mut self.messages);
            db.insert_batch(messages).map_err(|e| RuntimeError::ExportError(e))?;
            // db.flush().map_err(|e| RuntimeError::ExportError(e))?;
        }
        Ok(())
    }
}

impl<'a> Writer<'a> for DB<'a> {
    fn format_message(&self, message: &Message, indent_size: usize) -> Result<String, TableError> {
        let indent = String::from_iter((0..indent_size).map(|_| " "));
        // Data we want to write to a file
        let mut formatted_message = String::new();

        // Add message date
        self.add_line(&mut formatted_message, &self.get_time(message), &indent);

        // Add message sender
        self.add_line(
            &mut formatted_message,
            self.config.who(
                message.handle_id,
                message.is_from_me(),
                &message.destination_caller_id
            ),
            &indent
        );

        // If message was deleted, annotate it
        if message.is_deleted() {
            self.add_line(
                &mut formatted_message,
                "This message was deleted from the conversation!",
                &indent
            );
        }

        // Useful message metadata
        let message_parts = message.body();
        let mut attachments = Attachment::from_message(&self.config.db, message)?;
        let mut replies = message.get_replies(&self.config.db)?;

        // Index of where we are in the attachment Vector
        let mut attachment_index: usize = 0;

        // Render subject
        if let Some(subject) = &message.subject {
            self.add_line(&mut formatted_message, subject, &indent);
        }

        // Handle SharePlay
        if message.is_shareplay() {
            self.add_line(&mut formatted_message, self.format_shareplay(), &indent);
        }

        // Handle Shared Location
        if message.started_sharing_location() || message.stopped_sharing_location() {
            self.add_line(&mut formatted_message, self.format_shared_location(message), &indent);
        }

        // Generate the message body from it's components
        for (idx, message_part) in message_parts.iter().enumerate() {
            match message_part {
                // Fitness messages have a prefix that we need to replace with the opposite if who sent the message
                BubbleComponent::Text(text_attrs) => {
                    if let Some(text) = &message.text {
                        // Render edited message content, if applicable
                        if message.is_part_edited(idx) {
                            if let Some(edited_parts) = &message.edited_parts {
                                if
                                    let Some(edited) = self.format_edited(
                                        message,
                                        edited_parts,
                                        idx,
                                        &indent
                                    )
                                {
                                    self.add_line(&mut formatted_message, &edited, &indent);
                                };
                            }
                        } else {
                            let mut formatted_text = String::with_capacity(text.len());

                            for text_attr in text_attrs {
                                if
                                    let Some(message_content) = text.get(
                                        text_attr.start..text_attr.end
                                    )
                                {
                                    formatted_text.push_str(
                                        &self.format_attributed(message_content, &text_attr.effect)
                                    );
                                }
                            }

                            // If we failed to parse any text above, use the original text
                            if formatted_text.is_empty() {
                                formatted_text.push_str(text);
                            }

                            if formatted_text.starts_with(FITNESS_RECEIVER) {
                                self.add_line(
                                    &mut formatted_message,
                                    &formatted_text.replace(FITNESS_RECEIVER, YOU),
                                    &indent
                                );
                            } else {
                                self.add_line(&mut formatted_message, &formatted_text, &indent);
                            }
                        }
                    }
                }
                BubbleComponent::Attachment(_) =>
                    match attachments.get_mut(attachment_index) {
                        Some(attachment) => {
                            if attachment.is_sticker {
                                let result = self.format_sticker(attachment, message);
                                self.add_line(&mut formatted_message, &result, &indent);
                            } else {
                                match self.format_attachment(attachment, message) {
                                    Ok(result) => {
                                        attachment_index += 1;
                                        self.add_line(&mut formatted_message, &result, &indent);
                                    }
                                    Err(result) => {
                                        self.add_line(&mut formatted_message, result, &indent);
                                    }
                                }
                            }
                        }
                        // Attachment does not exist in attachments table
                        None =>
                            self.add_line(&mut formatted_message, "Attachment missing!", &indent),
                    }
                BubbleComponent::App =>
                    match self.format_app(message, &mut attachments, &indent) {
                        // We use an empty indent here because `format_app` handles building the entire message
                        Ok(ok_bubble) => self.add_line(&mut formatted_message, &ok_bubble, &indent),
                        Err(why) =>
                            self.add_line(
                                &mut formatted_message,
                                &format!("Unable to format app message: {why}"),
                                &indent
                            ),
                    }
                BubbleComponent::Retracted => {
                    if let Some(edited_parts) = &message.edited_parts {
                        if
                            let Some(edited) = self.format_edited(
                                message,
                                edited_parts,
                                idx,
                                &indent
                            )
                        {
                            self.add_line(&mut formatted_message, &edited, &indent);
                        };
                    }
                }
            }

            // Handle expressives
            if message.expressive_send_style_id.is_some() {
                self.add_line(&mut formatted_message, self.format_expressive(message), &indent);
            }

            // Handle Tapbacks
            if let Some(tapbacks_map) = self.config.tapbacks.get(&message.guid) {
                if let Some(tapbacks) = tapbacks_map.get(&idx) {
                    let mut formatted_tapbacks = String::new();
                    tapbacks.iter().try_for_each(
                        |tapbacks| -> Result<(), TableError> {
                            let formatted = self.format_tapback(tapbacks)?;
                            if !formatted.is_empty() {
                                self.add_line(
                                    &mut formatted_tapbacks,
                                    &self.format_tapback(tapbacks)?,
                                    &indent
                                );
                            }
                            Ok(())
                        }
                    )?;

                    if !formatted_tapbacks.is_empty() {
                        self.add_line(&mut formatted_message, "Tapbacks:", &indent);
                        self.add_line(&mut formatted_message, &formatted_tapbacks, &indent);
                    }
                }
            }

            // Handle Replies
            if let Some(replies) = replies.get_mut(&idx) {
                replies.iter_mut().try_for_each(
                    |reply| -> Result<(), TableError> {
                        let _ = reply.generate_text(&self.config.db);
                        if !reply.is_tapback() {
                            self.add_line(
                                &mut formatted_message,
                                &self.format_message(reply, 4)?,
                                &indent
                            );
                        }
                        Ok(())
                    }
                )?;
            }
        }

        // Add a note if the message is a reply
        if message.is_reply() && indent.is_empty() {
            self.add_line(
                &mut formatted_message,
                "This message responded to an earlier message.",
                &indent
            );
        }

        if indent.is_empty() {
            // Add a newline for top-level messages
            formatted_message.push('\n');
        }

        Ok(formatted_message)
    }

    fn format_attachment(
        &self,
        attachment: &'a mut Attachment,
        message: &Message
    ) -> Result<String, &'a str> {
        // Copy the file, if requested
        self.config.options.attachment_manager
            .handle_attachment(message, attachment, self.config)
            .ok_or(attachment.filename())?;

        // Build a relative filepath from the fully qualified one on the `Attachment`
        Ok(attachment.filename.clone().unwrap_or(self.config.message_attachment_path(attachment)))
    }

    fn format_sticker(&self, sticker: &'a mut Attachment, message: &Message) -> String {
        let who = self.config.who(
            message.handle_id,
            message.is_from_me(),
            &message.destination_caller_id
        );
        match self.format_attachment(sticker, message) {
            Ok(path_to_sticker) => {
                let sticker_effect = sticker.get_sticker_effect(
                    &self.config.options.platform,
                    &self.config.options.db_path,
                    self.config.options.attachment_root.as_deref()
                );
                if let Ok(Some(sticker_effect)) = sticker_effect {
                    return format!("{sticker_effect} Sticker from {who}: {path_to_sticker}");
                }
                format!("Sticker from {who}: {path_to_sticker}")
            }
            Err(path) => format!("Sticker from {who}: {path}"),
        }
    }

    fn format_app(
        &self,
        message: &'a Message,
        attachments: &mut Vec<Attachment>,
        indent: &str
    ) -> Result<String, PlistParseError> {
        if let Variant::App(balloon) = message.variant() {
            let mut app_bubble = String::new();

            // Handwritten messages use a different payload type, so check that first
            if message.is_handwriting() {
                if let Some(payload) = message.raw_payload_data(&self.config.db) {
                    return match HandwrittenMessage::from_payload(&payload) {
                        Ok(bubble) => Ok(self.format_handwriting(message, &bubble, indent)),
                        Err(why) => Err(PlistParseError::HandwritingError(why)),
                    };
                }
            }

            if let Some(payload) = message.payload_data(&self.config.db) {
                // Handle URL messages separately since they are a special case
                let res = if message.is_url() {
                    let parsed = parse_plist(&payload)?;
                    let bubble = URLMessage::get_url_message_override(&parsed)?;
                    match bubble {
                        URLOverride::Normal(balloon) => self.format_url(message, &balloon, indent),
                        URLOverride::AppleMusic(balloon) => self.format_music(&balloon, indent),
                        URLOverride::Collaboration(balloon) => {
                            self.format_collaboration(&balloon, indent)
                        }
                        URLOverride::AppStore(balloon) => self.format_app_store(&balloon, indent),
                        URLOverride::SharedPlacemark(balloon) => {
                            self.format_placemark(&balloon, indent)
                        }
                    }
                    // Handwriting uses a different payload type than the rest of the branches
                } else {
                    // Handle the app case
                    let parsed = parse_plist(&payload)?;
                    match AppMessage::from_map(&parsed) {
                        Ok(bubble) =>
                            match balloon {
                                CustomBalloon::Application(bundle_id) => {
                                    self.format_generic_app(&bubble, bundle_id, attachments, indent)
                                }
                                CustomBalloon::ApplePay => self.format_apple_pay(&bubble, indent),
                                CustomBalloon::Fitness => self.format_fitness(&bubble, indent),
                                CustomBalloon::Slideshow => self.format_slideshow(&bubble, indent),
                                CustomBalloon::CheckIn => self.format_check_in(&bubble, indent),
                                CustomBalloon::FindMy => self.format_find_my(&bubble, indent),
                                CustomBalloon::Handwriting => unreachable!(),
                                CustomBalloon::URL => unreachable!(),
                            }
                        Err(why) => {
                            return Err(why);
                        }
                    }
                };
                app_bubble.push_str(&res);
            } else {
                // Sometimes, URL messages are missing their payloads
                if message.is_url() {
                    if let Some(text) = &message.text {
                        return Ok(text.to_string());
                    }
                }
                return Err(PlistParseError::NoPayload);
            }
            Ok(app_bubble)
        } else {
            Err(PlistParseError::WrongMessageType)
        }
    }

    fn format_tapback(&self, msg: &Message) -> Result<String, TableError> {
        match msg.variant() {
            Variant::Tapback(_, added, tapback) => {
                if !added {
                    return Ok(String::new());
                }
                Ok(
                    format!(
                        "{} by {}",
                        tapback,
                        self.config.who(msg.handle_id, msg.is_from_me(), &msg.destination_caller_id)
                    )
                )
            }
            Variant::Sticker(_) => {
                let mut paths = Attachment::from_message(&self.config.db, msg)?;
                let who = self.config.who(
                    msg.handle_id,
                    msg.is_from_me(),
                    &msg.destination_caller_id
                );
                // Sticker messages have only one attachment, the sticker image
                Ok(
                    if let Some(sticker) = paths.get_mut(0) {
                        format!("{} from {who}", self.format_sticker(sticker, msg))
                    } else {
                        format!("Sticker from {who} not found!")
                    }
                )
            }
            _ => unreachable!(),
        }
    }

    fn format_expressive(&self, msg: &'a Message) -> &'a str {
        match msg.get_expressive() {
            Expressive::Screen(effect) =>
                match effect {
                    ScreenEffect::Confetti => "Sent with Confetti",
                    ScreenEffect::Echo => "Sent with Echo",
                    ScreenEffect::Fireworks => "Sent with Fireworks",
                    ScreenEffect::Balloons => "Sent with Balloons",
                    ScreenEffect::Heart => "Sent with Heart",
                    ScreenEffect::Lasers => "Sent with Lasers",
                    ScreenEffect::ShootingStar => "Sent with Shooting Star",
                    ScreenEffect::Sparkles => "Sent with Sparkles",
                    ScreenEffect::Spotlight => "Sent with Spotlight",
                }
            Expressive::Bubble(effect) =>
                match effect {
                    BubbleEffect::Slam => "Sent with Slam",
                    BubbleEffect::Loud => "Sent with Loud",
                    BubbleEffect::Gentle => "Sent with Gentle",
                    BubbleEffect::InvisibleInk => "Sent with Invisible Ink",
                }
            Expressive::Unknown(effect) => effect,
            Expressive::None => "",
        }
    }

    fn format_announcement(&self, msg: &'a Message) -> String {
        let mut who = self.config.who(msg.handle_id, msg.is_from_me(), &msg.destination_caller_id);
        // Rename yourself so we render the proper grammar here
        if who == ME {
            who = self.config.options.custom_name.as_deref().unwrap_or(YOU);
        }

        let timestamp = format(&msg.date(&self.config.offset));

        return match msg.get_announcement() {
            Some(announcement) =>
                match announcement {
                    Announcement::NameChange(name) => {
                        format!("{timestamp} {who} renamed the conversation to {name}\n\n")
                    }
                    Announcement::PhotoChange => {
                        format!("{timestamp} {who} changed the group photo.\n\n")
                    }
                    Announcement::Unknown(num) => {
                        format!("{timestamp} {who} performed unknown action {num}.\n\n")
                    }
                    Announcement::FullyUnsent => format!("{timestamp} {who} unsent a message!\n\n"),
                }
            None => String::from("Unable to format announcement!\n\n"),
        };
    }

    fn format_shareplay(&self) -> &str {
        "SharePlay Message\nEnded"
    }

    fn format_shared_location(&self, msg: &'a Message) -> &str {
        // Handle Shared Location
        if msg.started_sharing_location() {
            return "Started sharing location!";
        } else if msg.stopped_sharing_location() {
            return "Stopped sharing location!";
        }
        "Shared location!"
    }

    fn format_edited(
        &self,
        msg: &'a Message,
        edited_message: &'a EditedMessage,
        message_part_idx: usize,
        indent: &str
    ) -> Option<String> {
        if let Some(edited_message_part) = edited_message.part(message_part_idx) {
            let mut out_s = String::new();
            let mut previous_timestamp: Option<&i64> = None;

            match edited_message_part.status {
                EditStatus::Edited => {
                    for event in &edited_message_part.edit_history {
                        match previous_timestamp {
                            // Original message get an absolute timestamp
                            None => {
                                let parsed_timestamp = format(
                                    &get_local_time(&event.date, &self.config.offset)
                                );
                                out_s.push_str(&parsed_timestamp);
                                out_s.push(' ');
                            }
                            // Subsequent edits get a relative timestamp
                            Some(prev_timestamp) => {
                                let end = get_local_time(&event.date, &self.config.offset);
                                let start = get_local_time(prev_timestamp, &self.config.offset);
                                if let Some(diff) = readable_diff(start, end) {
                                    out_s.push_str(indent);
                                    out_s.push_str("Edited ");
                                    out_s.push_str(&diff);
                                    out_s.push_str(" later: ");
                                }
                            }
                        }

                        // Update the previous timestamp for the next loop
                        previous_timestamp = Some(&event.date);

                        // Render the message text
                        self.add_line(&mut out_s, &event.text, indent);
                    }
                }
                EditStatus::Unsent => {
                    let who = if msg.is_from_me() {
                        self.config.options.custom_name.as_deref().unwrap_or(YOU)
                    } else {
                        "They"
                    };

                    match
                        readable_diff(
                            msg.date(&self.config.offset),
                            msg.date_edited(&self.config.offset)
                        )
                    {
                        Some(diff) => {
                            out_s.push_str(who);
                            out_s.push_str(" unsent this message part ");
                            out_s.push_str(&diff);
                            out_s.push_str(" after sending!");
                        }
                        None => {
                            out_s.push_str(who);
                            out_s.push_str(" unsent this message part!");
                        }
                    }
                }
                EditStatus::Original => {
                    return None;
                }
            }

            return Some(out_s);
        }
        None
    }

    fn format_attributed(&'a self, msg: &'a str, _: &'a TextEffect) -> Cow<str> {
        // There isn't really a way to represent formatted text in a plain text export
        Cow::Borrowed(msg)
    }

    fn write_to_file(file: &mut BufWriter<File>, text: &str) -> Result<(), RuntimeError> {
        file.write_all(text.as_bytes()).map_err(RuntimeError::DiskError)
    }
}

impl<'a> BalloonFormatter<&'a str> for DB<'a> {
    fn format_url(&self, msg: &Message, balloon: &URLMessage, indent: &str) -> String {
        let mut out_s = String::new();

        if let Some(url) = balloon.get_url() {
            self.add_line(&mut out_s, url, indent);
        } else if let Some(text) = &msg.text {
            self.add_line(&mut out_s, text, indent);
        }

        if let Some(title) = balloon.title {
            self.add_line(&mut out_s, title, indent);
        }

        if let Some(summary) = balloon.summary {
            self.add_line(&mut out_s, summary, indent);
        }

        // We want to keep the newlines between blocks, but the last one should be removed
        out_s.strip_suffix('\n').unwrap_or(&out_s).to_string()
    }

    fn format_music(&self, balloon: &MusicMessage, indent: &str) -> String {
        let mut out_s = String::new();

        if let Some(track_name) = balloon.track_name {
            self.add_line(&mut out_s, track_name, indent);
        }

        if let Some(album) = balloon.album {
            self.add_line(&mut out_s, album, indent);
        }

        if let Some(artist) = balloon.artist {
            self.add_line(&mut out_s, artist, indent);
        }

        if let Some(url) = balloon.url {
            self.add_line(&mut out_s, url, indent);
        }

        out_s
    }

    fn format_collaboration(&self, balloon: &CollaborationMessage, indent: &str) -> String {
        let mut out_s = String::from(indent);

        if let Some(name) = balloon.app_name {
            out_s.push_str(name);
        } else if let Some(bundle_id) = balloon.bundle_id {
            out_s.push_str(bundle_id);
        }

        if !out_s.is_empty() {
            out_s.push_str(" message:\n");
        }

        if let Some(title) = balloon.title {
            self.add_line(&mut out_s, title, indent);
        }

        if let Some(url) = balloon.get_url() {
            self.add_line(&mut out_s, url, indent);
        }

        // We want to keep the newlines between blocks, but the last one should be removed
        out_s.strip_suffix('\n').unwrap_or(&out_s).to_string()
    }

    fn format_app_store(&self, balloon: &AppStoreMessage, indent: &'a str) -> String {
        let mut out_s = String::from(indent);

        if let Some(name) = balloon.app_name {
            self.add_line(&mut out_s, name, indent);
        }

        if let Some(description) = balloon.description {
            self.add_line(&mut out_s, description, indent);
        }

        if let Some(platform) = balloon.platform {
            self.add_line(&mut out_s, platform, indent);
        }

        if let Some(genre) = balloon.genre {
            self.add_line(&mut out_s, genre, indent);
        }

        if let Some(url) = balloon.url {
            self.add_line(&mut out_s, url, indent);
        }

        // We want to keep the newlines between blocks, but the last one should be removed
        out_s.strip_suffix('\n').unwrap_or(&out_s).to_string()
    }

    fn format_placemark(&self, balloon: &PlacemarkMessage, indent: &'a str) -> String {
        let mut out_s = String::from(indent);

        if let Some(name) = balloon.place_name {
            self.add_line(&mut out_s, name, indent);
        }

        if let Some(url) = balloon.get_url() {
            self.add_line(&mut out_s, url, indent);
        }

        if let Some(name) = balloon.placemark.name {
            self.add_line(&mut out_s, name, indent);
        }

        if let Some(address) = balloon.placemark.address {
            self.add_line(&mut out_s, address, indent);
        }

        if let Some(state) = balloon.placemark.state {
            self.add_line(&mut out_s, state, indent);
        }

        if let Some(city) = balloon.placemark.city {
            self.add_line(&mut out_s, city, indent);
        }

        if let Some(iso_country_code) = balloon.placemark.iso_country_code {
            self.add_line(&mut out_s, iso_country_code, indent);
        }

        if let Some(postal_code) = balloon.placemark.postal_code {
            self.add_line(&mut out_s, postal_code, indent);
        }

        if let Some(country) = balloon.placemark.country {
            self.add_line(&mut out_s, country, indent);
        }

        if let Some(street) = balloon.placemark.street {
            self.add_line(&mut out_s, street, indent);
        }

        if let Some(sub_administrative_area) = balloon.placemark.sub_administrative_area {
            self.add_line(&mut out_s, sub_administrative_area, indent);
        }

        if let Some(sub_locality) = balloon.placemark.sub_locality {
            self.add_line(&mut out_s, sub_locality, indent);
        }

        // We want to keep the newlines between blocks, but the last one should be removed
        out_s.strip_suffix('\n').unwrap_or(&out_s).to_string()
    }

    fn format_handwriting(
        &self,
        msg: &Message,
        balloon: &HandwrittenMessage,
        indent: &str
    ) -> String {
        match self.config.options.attachment_manager {
            AttachmentManager::Disabled =>
                balloon.render_ascii(40).replace("\n", &format!("{indent}\n")),
            AttachmentManager::Compatible | AttachmentManager::Efficient =>
                self.config.options.attachment_manager
                    .handle_handwriting(msg, balloon, self.config)
                    .map(|filepath| {
                        self.config
                            .relative_path(PathBuf::from(&filepath))
                            .unwrap_or(filepath.display().to_string())
                    })
                    .map(|filepath| format!("{indent}{filepath}"))
                    .unwrap_or_else(|| {
                        balloon.render_ascii(40).replace("\n", &format!("{indent}\n"))
                    }),
        }
    }

    fn format_apple_pay(&self, balloon: &AppMessage, indent: &str) -> String {
        let mut out_s = String::from(indent);
        if let Some(caption) = balloon.caption {
            out_s.push_str(caption);
            out_s.push_str(" transaction: ");
        }

        if let Some(ldtext) = balloon.ldtext {
            out_s.push_str(ldtext);
        } else {
            out_s.push_str("unknown amount");
        }

        out_s
    }

    fn format_fitness(&self, balloon: &AppMessage, indent: &str) -> String {
        let mut out_s = String::from(indent);
        if let Some(app_name) = balloon.app_name {
            out_s.push_str(app_name);
            out_s.push_str(" message: ");
        }
        if let Some(ldtext) = balloon.ldtext {
            out_s.push_str(ldtext);
        } else {
            out_s.push_str("unknown workout");
        }
        out_s
    }

    fn format_slideshow(&self, balloon: &AppMessage, indent: &str) -> String {
        let mut out_s = String::from(indent);
        if let Some(ldtext) = balloon.ldtext {
            out_s.push_str("Photo album: ");
            out_s.push_str(ldtext);
        }

        if let Some(url) = balloon.url {
            out_s.push(' ');
            out_s.push_str(url);
        }

        out_s
    }

    fn format_find_my(&self, balloon: &AppMessage, indent: &'a str) -> String {
        let mut out_s = String::from(indent);
        if let Some(app_name) = balloon.app_name {
            out_s.push_str(app_name);
            out_s.push_str(": ");
        }

        if let Some(ldtext) = balloon.ldtext {
            out_s.push(' ');
            out_s.push_str(ldtext);
        }

        out_s
    }

    fn format_check_in(&self, balloon: &AppMessage, indent: &'a str) -> String {
        let mut out_s = String::from(indent);

        out_s.push_str(balloon.caption.unwrap_or("Check In"));

        let metadata: HashMap<&str, &str> = balloon.parse_query_string();

        // Before manual check-in
        if let Some(date_str) = metadata.get("estimatedEndTime") {
            // Parse the estimated end time from the message's query string
            let date_stamp = (date_str.parse::<f64>().unwrap_or(0.0) as i64) * TIMESTAMP_FACTOR;
            let date_time = get_local_time(&date_stamp, &0);
            let date_string = format(&date_time);

            out_s.push_str("\nExpected at ");
            out_s.push_str(&date_string);
        } else if
            // Expired check-in
            let Some(date_str) = metadata.get("triggerTime")
        {
            // Parse the estimated end time from the message's query string
            let date_stamp = (date_str.parse::<f64>().unwrap_or(0.0) as i64) * TIMESTAMP_FACTOR;
            let date_time = get_local_time(&date_stamp, &0);
            let date_string = format(&date_time);

            out_s.push_str("\nWas expected at ");
            out_s.push_str(&date_string);
        } else if
            // Accepted check-in
            let Some(date_str) = metadata.get("sendDate")
        {
            // Parse the estimated end time from the message's query string
            let date_stamp = (date_str.parse::<f64>().unwrap_or(0.0) as i64) * TIMESTAMP_FACTOR;
            let date_time = get_local_time(&date_stamp, &0);
            let date_string = format(&date_time);

            out_s.push_str("\nChecked in at ");
            out_s.push_str(&date_string);
        }

        out_s
    }

    fn format_generic_app(
        &self,
        balloon: &AppMessage,
        bundle_id: &str,
        _: &mut Vec<Attachment>,
        indent: &str
    ) -> String {
        let mut out_s = String::from(indent);

        if let Some(name) = balloon.app_name {
            out_s.push_str(name);
        } else {
            out_s.push_str(bundle_id);
        }

        if !out_s.is_empty() {
            out_s.push_str(" message:\n");
        }

        if let Some(title) = balloon.title {
            self.add_line(&mut out_s, title, indent);
        }

        if let Some(subtitle) = balloon.subtitle {
            self.add_line(&mut out_s, subtitle, indent);
        }

        if let Some(caption) = balloon.caption {
            self.add_line(&mut out_s, caption, indent);
        }

        if let Some(subcaption) = balloon.subcaption {
            self.add_line(&mut out_s, subcaption, indent);
        }

        if let Some(trailing_caption) = balloon.trailing_caption {
            self.add_line(&mut out_s, trailing_caption, indent);
        }

        if let Some(trailing_subcaption) = balloon.trailing_subcaption {
            self.add_line(&mut out_s, trailing_subcaption, indent);
        }

        // We want to keep the newlines between blocks, but the last one should be removed
        out_s.strip_suffix('\n').unwrap_or(&out_s).to_string()
    }
}
