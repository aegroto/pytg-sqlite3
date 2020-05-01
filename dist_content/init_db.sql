CREATE TABLE IF NOT EXISTS Users (
    chat_id PRIMARY KEY,
    username,
    authorized INTEGER DEFAULT 0
);