-- dedicated feedback database
CREATE DATABASE feedback_db;

-- connect and create the table
\c feedback_db;

CREATE TABLE IF NOT EXISTS user_feedback (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP WITH TIME ZONE,
    user_query TEXT NOT NULL,
    llm_response TEXT NOT NULL,
    rating INT NOT NULL, -- 0 = Thumbs Down, 1 = Thumbs Up
    comment TEXT
);