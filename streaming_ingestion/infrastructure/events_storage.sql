\c events_storage;

CREATE TABLE IF NOT EXISTS raw_events (
    batch_id VARCHAR(64),
    `value` JSONB NOT NULL, 
    topic VARCHAR(100) NOT NULL,
    `partition` INT NOT NULL,
    offset BIGINT NOT NULL,
    `timestamp` TIMESTAMP NOT NULL,
    PRIMARY KEY (partition, offset)
);

CREATE TABLE IF NOT EXISTS parsed_events (
    batch_id VARCHAR(64),
    user_id BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    game_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    PRIMARY KEY (user_id, timestamp, event_type)
);


CREATE TABLE IF NOT EXISTS event_counts (
    batch_id VARCHAR(64),
    event_type VARCHAR(100) NOT NULL,
    occurrences BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_event_counts (
    batch_id VARCHAR(64),
    event_type VARCHAR(100) NOT NULL,
    user_id BIGINT NOT NULL,
    occurrences BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_avg_waiting_time (
    batch_id VARCHAR(64),
    user_id BIGINT NOT NULL,
    avg_waiting_time DECIMAL(10, 2) NOT NULL
);
