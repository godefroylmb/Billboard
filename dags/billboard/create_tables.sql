-- SQL Script to create Billboard chart tables
CREATE TABLE IF NOT EXISTS hot100 (
    id SERIAL PRIMARY KEY,
    chart_date DATE NOT NULL,
    song_name VARCHAR(500) NOT NULL,
    artist VARCHAR(500) NOT NULL,
    rank INTEGER NOT NULL,
    last_week VARCHAR(10),
    peak_position VARCHAR(10),
    weeks_in_chart VARCHAR(10),
    image_url TEXT,
    UNIQUE (chart_date, rank)
);

-- Global 200 Table
CREATE TABLE IF NOT EXISTS global200 (
    id SERIAL PRIMARY KEY,
    chart_date DATE NOT NULL,
    song_name VARCHAR(500) NOT NULL,
    artist VARCHAR(500) NOT NULL,
    rank INTEGER NOT NULL,
    last_week VARCHAR(10),
    peak_position VARCHAR(10),
    weeks_in_chart VARCHAR(10),
    image_url TEXT,
    UNIQUE (chart_date, rank)
);

-- Radio Songs Table
CREATE TABLE IF NOT EXISTS radio (
    id SERIAL PRIMARY KEY,
    chart_date DATE NOT NULL,
    song_name VARCHAR(500) NOT NULL,
    artist VARCHAR(500) NOT NULL,
    rank INTEGER NOT NULL,
    last_week VARCHAR(10),
    peak_position VARCHAR(10),
    weeks_in_chart VARCHAR(10),
    image_url TEXT,
    UNIQUE (chart_date, rank)
);

-- Streaming Songs Table
CREATE TABLE IF NOT EXISTS streaming (
    id SERIAL PRIMARY KEY,
    chart_date DATE NOT NULL,
    song_name VARCHAR(500) NOT NULL,
    artist VARCHAR(500) NOT NULL,
    rank INTEGER NOT NULL,
    last_week VARCHAR(10),
    peak_position VARCHAR(10),
    weeks_in_chart VARCHAR(10),
    image_url TEXT,
    UNIQUE (chart_date, rank)
);

-- Digital Sales Table
CREATE TABLE IF NOT EXISTS digital (
    id SERIAL PRIMARY KEY,
    chart_date DATE NOT NULL,
    song_name VARCHAR(500) NOT NULL,
    artist VARCHAR(500) NOT NULL,
    rank INTEGER NOT NULL,
    last_week VARCHAR(10),
    peak_position VARCHAR(10),
    weeks_in_chart VARCHAR(10),
    image_url TEXT,
    UNIQUE (chart_date, rank)
);
