use crate::Message;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestRecord {
    pub id: Option<String>,
    pub rowid: i32,
    pub guid: String,
    pub text: Option<String>,
    pub service: Option<String>,
    pub platform: String,
    pub handle_id: Option<i32>,
    pub destination_caller_id: Option<String>,
    pub subject: Option<String>,
    pub date: Option<i64>,
    pub date_read: Option<i64>,
    pub date_delivered: Option<i64>,
    pub is_from_me: bool,
    pub is_read: bool,
    pub item_type: i32,
    pub other_handle: Option<i32>,
    pub share_status: bool,
    pub share_direction: Option<bool>,
    pub group_title: Option<String>,
    pub group_action_type: i32,
    pub associated_message_guid: Option<String>,
    pub associated_message_type: Option<i32>,
    pub balloon_bundle_id: Option<String>,
    pub expressive_send_style_id: Option<String>,
    pub thread_originator_guid: Option<String>,
    pub thread_originator_part: Option<String>,
    pub date_edited: Option<i64>,
    pub chat_id: Option<i32>,
    pub unique_chat_id: String,
    pub num_attachments: i32,
    pub deleted_from: Option<i32>,
    pub num_replies: i32,
    pub full_message: String,
    pub thread_name: Option<String>,
    pub attachment_paths: Vec<String>,
    pub is_deleted: bool,
    pub is_edited: bool,
    pub is_reply: bool,
    pub associated_message_emoji: Option<String>,
    pub phone_number: String,
}

impl From<Message> for IngestRecord {
    fn from(msg: Message) -> Self {
        Self {
            id: msg.id,
            rowid: msg.rowid,
            guid: msg.guid,
            text: msg.text,
            service: msg.service,
            platform: msg.platform,
            handle_id: msg.handle_id,
            destination_caller_id: msg.destination_caller_id,
            subject: msg.subject,
            date: datetime_to_micros(msg.date),
            date_read: datetime_to_micros(msg.date_read),
            date_delivered: datetime_to_micros(msg.date_delivered),
            is_from_me: msg.is_from_me,
            is_read: msg.is_read,
            item_type: msg.item_type,
            other_handle: msg.other_handle,
            share_status: msg.share_status,
            share_direction: msg.share_direction,
            group_title: msg.group_title,
            group_action_type: msg.group_action_type,
            associated_message_guid: msg.associated_message_guid,
            associated_message_type: msg.associated_message_type,
            balloon_bundle_id: msg.balloon_bundle_id,
            expressive_send_style_id: msg.expressive_send_style_id,
            thread_originator_guid: msg.thread_originator_guid,
            thread_originator_part: msg.thread_originator_part,
            date_edited: datetime_to_micros(msg.date_edited),
            chat_id: msg.chat_id,
            unique_chat_id: msg.unique_chat_id,
            num_attachments: msg.num_attachments,
            deleted_from: msg.deleted_from,
            num_replies: msg.num_replies,
            full_message: msg.full_message,
            thread_name: msg.thread_name,
            attachment_paths: msg.attachment_paths,
            is_deleted: msg.is_deleted,
            is_edited: msg.is_edited,
            is_reply: msg.is_reply,
            associated_message_emoji: msg.associated_message_emoji,
            phone_number: msg.phone_number,
        }
    }
}

impl From<IngestRecord> for Message {
    fn from(record: IngestRecord) -> Self {
        Self {
            id: record.id,
            rowid: record.rowid,
            guid: record.guid,
            text: record.text,
            service: record.service,
            platform: record.platform,
            handle_id: record.handle_id,
            destination_caller_id: record.destination_caller_id,
            subject: record.subject,
            date: micros_to_datetime(record.date),
            date_read: micros_to_datetime(record.date_read),
            date_delivered: micros_to_datetime(record.date_delivered),
            is_from_me: record.is_from_me,
            is_read: record.is_read,
            item_type: record.item_type,
            other_handle: record.other_handle,
            share_status: record.share_status,
            share_direction: record.share_direction,
            group_title: record.group_title,
            group_action_type: record.group_action_type,
            associated_message_guid: record.associated_message_guid,
            associated_message_type: record.associated_message_type,
            balloon_bundle_id: record.balloon_bundle_id,
            expressive_send_style_id: record.expressive_send_style_id,
            thread_originator_guid: record.thread_originator_guid,
            thread_originator_part: record.thread_originator_part,
            date_edited: micros_to_datetime(record.date_edited),
            chat_id: record.chat_id,
            unique_chat_id: record.unique_chat_id,
            num_attachments: record.num_attachments,
            deleted_from: record.deleted_from,
            num_replies: record.num_replies,
            full_message: record.full_message,
            thread_name: record.thread_name,
            attachment_paths: record.attachment_paths,
            is_deleted: record.is_deleted,
            is_edited: record.is_edited,
            is_reply: record.is_reply,
            associated_message_emoji: record.associated_message_emoji,
            phone_number: record.phone_number,
        }
    }
}

fn datetime_to_micros(value: Option<DateTime<Utc>>) -> Option<i64> {
    value.map(|dt| dt.timestamp_micros())
}

pub fn micros_to_datetime(value: Option<i64>) -> Option<DateTime<Utc>> {
    value.and_then(|micros| {
        let secs = micros.div_euclid(1_000_000);
        let micros_rem = micros.rem_euclid(1_000_000);
        let nanos = (micros_rem as u32) * 1_000;
        Utc.timestamp_opt(secs, nanos).single()
    })
}
