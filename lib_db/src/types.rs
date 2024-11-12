use serde::{ Serialize, Deserialize };
use surrealdb::sql::Thing;

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Thing>,
    name: String,
    value: i32,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Thing>,
    pub rowid: i32,
    pub guid: String,
    pub text: Option<String>,
    pub service: Option<String>,
    pub handle_id: Option<i32>,
    pub destination_caller_id: Option<String>,
    pub subject: Option<String>,
    pub date: String,
    pub date_read: String,
    pub date_delivered: String,
    pub is_from_me: bool,
    pub is_read: bool,
    pub item_type: i32,
    pub other_handle: i32,
    pub share_status: bool,
    pub share_direction: bool,
    pub group_title: Option<String>,
    pub group_action_type: i32,
    pub associated_message_guid: Option<String>,
    pub associated_message_type: Option<i32>,
    pub balloon_bundle_id: Option<String>,
    pub expressive_send_style_id: Option<String>,
    pub thread_originator_guid: Option<String>,
    pub thread_originator_part: Option<String>,
    pub date_edited: String,
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
