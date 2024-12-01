-- Define tables
DEFINE TABLE persons SCHEMALESS;
DEFINE TABLE threads SCHEMALESS;
DEFINE TABLE messages SCHEMALESS;


-- Message indexes
DEFINE INDEX message_date ON messages FIELDS date;
DEFINE INDEX message_guids ON messages FIELDS guid;
DEFINE INDEX message_unique_chat_id ON messages FIELDS unique_chat_id;
DEFINE INDEX message_phone_number ON messages FIELDS phone_number;
DEFINE INDEX message_phone_unique_chat_id ON messages FIELDS phone_number, unique_chat_id;
-- DEFINE INDEX message_is_from_me ON messages FIELDS is_from_me;
-- DEFINE INDEX message_platform_date ON messages FIELDS platform, date;
-- DEFINE INDEX message_platform_text ON messages FIELDS platform, text;
-- DEFINE INDEX message_date_is_from_me ON messages FIELDS date, is_from_me;
-- DEFINE INDEX message_thread_id ON messages FIELDS thread_id;
-- DEFINE INDEX message_thread_is_from_me ON messages FIELDS thread_id, is_from_me;
