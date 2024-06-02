import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "Drop table if exists staging_events"
staging_songs_table_drop = "Drop table if exists staging_songs"
songplay_table_drop = "Drop table if exists songplay"
user_table_drop = "Drop table if exists user"
song_table_drop = "Drop table if exists song"
artist_table_drop = "Drop table if exists artist"
time_table_drop = "Drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist          varchar, 
    auth            varchar, 
    firstName       varchar, 
    gender          char(1),
    itemInSession   int, 
    lastName        varchar,
    length          float,
    level           varchar, 
    location        text, 
    method          varchar,
    page            varchar,
    registration    float, 
    sessionId       int, 
    song            varchar,
    status          int,
    ts              bigint,
    userAgent       text, 
    userId          varchar               
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs           int,
    artist_id           varchar,
    artist_latitude     float,
    artist_longitude    float, 
    artist_location     text, 
    artist_name         varchar, 
    song_id             varchar, 
    title               varchar,
    year                int,
    duration            float
);
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id         INT IDENTITY(0,1) PRIMARY KEY,
    start_time          timestamp NOT NULL, 
    user_id             varchar NOT NULL,
    level               varchar,
    song_id             varchar NOT NULL, 
    artist_id           varchar NOT NULL, 
    session_id          int, 
    location            text, 
    user_agent          text
);
""")

user_table_create = ("""
CREATE TABLE user (
    user_id             varchar PRIMARY KEY,
    first_name          varchar, 
    last_name           varchar, 
    gender              char(1), 
    level               varchar
);

""")

song_table_create = ("""
CREATE TABLE song (
    song_id             varchar PRIMARY KEY,
    title               varchar,
    artist_id           varchar NOT NULL,
    year                int, 
    duration            float
);
""")

artist_table_create = ("""
CREATE TABLE artist(
    artist_id           varchar PRIMARY KEY, 
    name                varchar, 
    location            text, 
    latitude            float,
    longitude           float
);
""")

time_table_create = ("""
CREATE TABLE time(
    start_time          timestamp PRIMARY KEY, 
    hour                int,
    day                 int,
    week                int,
    month               int,
    year                int,
    weekday             int
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {}
    iam_role {}
    json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT
            TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            se.userId       AS user_id,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId    AS session_id,
            se.location,
            se.userAgent    AS user_agent
        FROM staging_events se
        JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
        WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO user (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId      AS user_id,
        firstName   AS first_name,
        lastName    AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(weekday FROM start_time)    AS weekday
    FROM staging_events
    WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
