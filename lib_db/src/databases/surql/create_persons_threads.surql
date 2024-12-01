BEGIN TRANSACTION;

-- Store all messages with their grouping
LET $all = (
    SELECT
        id,
        phone_number,
        unique_chat_id
    FROM messages
    GROUP BY phone_number, unique_chat_id
);

-- Create persons and store results
LET $persons = array::distinct($all.phone_number);

FOR $phone IN $persons {
    LET $person = (CREATE persons CONTENT { 
        phone_number: $phone, 
        is_me: false
    })[0];
};

-- Create threads and store results
LET $chats = array::distinct($all.unique_chat_id);

FOR $chat IN $chats {
    LET $thread = (CREATE threads CONTENT { 
        unique_chat_id: $chat
    })[0];
};

-- Return counts for verification
RETURN {
    thread_count: count((SELECT * FROM threads)),
    person_count: count((SELECT * FROM persons)),
    in_thread_count: count((SELECT * FROM in_thread)),
    sent_count: count((SELECT * FROM sent)),
    message_count: count((SELECT * FROM messages)),
    messaged_in_count: count((SELECT * FROM messaged_in))
};

COMMIT TRANSACTION;

