CREATE DATABASE IF NOT EXISTS events_storage;

USE events_storage;

-- Contagem total de eventos por tipo (exemplo: clique, login, compra)
CREATE TABLE IF NOT EXISTS event_counts(
	event_type VARCHAR(100) NOT NULL,
    occurrences BIGINT DEFAULT 0,
    PRIMARY KEY (event_type)
);

-- Contagem de usuários únicos que realizaram cada tipo de evento
CREATE TABLE IF NOT EXISTS user_event_counts(
  event_type VARCHAR(100) NOT NULL,
    `user_id` BIGINT NOT NULL,
    occurrences BIGINT DEFAULT 0,
    PRIMARY KEY (event_type, `user_id`)
);

-- Tempo médio entre eventos por usuário
CREATE TABLE IF NOT EXISTS user_avg_waiting_time(
  `user_id` BIGINT NOT NULL,
    avg_waiting_time DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (`user_id`)
);
