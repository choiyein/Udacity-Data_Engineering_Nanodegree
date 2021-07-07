import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
                                (
                                artist        VARCHAR,
                                auth          VARCHAR,
                                firstName     VARCHAR,
                                gender        CHAR(1),
                                itemInSession SMALLINT,
                                lastName      VARCHAR,
                                length        NUMERIC,
                                level         VARCHAR,
                                location      VARCHAR,
                                method        VARCHAR,
                                page          VARCHAR,
                                registration  DECIMAL(20, 1),
                                sessionId     SMALLINT,
                                song          VARCHAR,
                                status        SMALLINT,
                                ts            TIMESTAMP,
                                userAgent     VARCHAR,
                                userId        INTEGER
                                )
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                (
                                num_songs         SMALLINT, 
                                artist_id         VARCHAR, 
                                artist_latitude   DECIMAL(10,6), 
                                artist_longitude  DECIMAL(10,6),
                                artist_location   VARCHAR, 
                                artist_name       VARCHAR,
                                song_id           VARCHAR, 
                                title             VARCHAR, 
                                duration          DECIMAL(10,6),
                                year              SMALLINT
                                )
                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                        (
                        user_id     INTEGER  NOT NULL, 
                        first_name  VARCHAR, 
                        last_name   VARCHAR, 
                        gender      CHAR(1), 
                        level       VARCHAR,

                        PRIMARY KEY(user_id)
                        )
                        DISTSTYLE ALL;
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (
                        song_id   VARCHAR       NOT NULL,
                        title     VARCHAR, 
                        artist_id VARCHAR       NOT NULL     DISTKEY, 
                        year      INTEGER, 
                        duration  DECIMAL(10,6),

                        PRIMARY KEY(song_id)
                        );
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                          (
                            artist_id VARCHAR      NOT NULL   DISTKEY,
                            name      VARCHAR, 
                            location  VARCHAR, 
                            latitude  DECIMAL(10,6), 
                            longitude DECIMAL(10,6),

                            PRIMARY KEY(artist_id)  
                          );
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (
                        start_time TIMESTAMP   NOT NULL   DISTKEY, 
                        hour       SMALLINT    NOT NULL, 
                        day        SMALLINT    NOT NULL, 
                        week       SMALLINT    NOT NULL, 
                        month      SMALLINT    NOT NULL, 
                        year       SMALLINT    NOT NULL, 
                        weekday    SMALLINT    NOT NULL,

                        PRIMARY KEY(start_time) 
                        );
                    """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                            (
                            songplay_id  INTEGER        NOT NULL   IDENTITY(0,1), 
                            start_time   TIMESTAMP      NOT NULL,
                            user_id      INTEGER        NOT NULL,
                            level        VARCHAR,
                            song_id      VARCHAR        NOT NULL,
                            artist_id    VARCHAR        NOT NULL,
                            session_id   SMALLINT,
                            location     VARCHAR,
                            user_agent   VARCHAR,

                            PRIMARY KEY(songplay_id)
                            )
                            DISTSTYLE ALL;
                        """)


# STAGING TABLES

staging_events_copy = ("""
                       COPY staging_events 
                       FROM {}
                       CREDENTIALS 'aws_iam_role={}' 
                       REGION 'us-west-2'
                       FORMAT as JSON {} 
                       TIMEFORMAT as 'epochmillisecs';
                       """).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
                       COPY staging_songs 
                       FROM {}
                       CREDENTIALS 'aws_iam_role={}' 
                       REGION 'us-west-2'
                       FORMAT as JSON 'auto';
                       """).format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT ts         AS start_time, 
                                   userId     AS user_id, 
                                   level, 
                                   song_id, 
                                   artist_id, 
                                   sessionId  AS session_id, 
                                   location, 
                                   userAgent  AS user_agent
                            FROM staging_events e 
                            JOIN staging_songs s
                            ON (e.artist = s.artist_name AND e.song = s.title)
                            WHERE page = 'NextSong';
                        """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                        SELECT DISTINCT userId                              AS user_id, 
                               firstName                                    AS first_name, 
                               lastName                                     AS last_name, 
                               gender, 
                               LAST_VALUE(level) OVER (PARTITION BY userId ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS level
                        FROM staging_events
                        WHERE page = 'NextSong';
                    """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, 
                               title, 
                               artist_id, 
                               year, 
                               duration
                        FROM staging_songs;
                    """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                          SELECT DISTINCT artist_id,
                                 artist_name        AS name, 
                                 artist_location    AS location, 
                                 artist_latitude    AS latitude, 
                                 artist_longitude   AS longitude
                          FROM staging_songs;
                      """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                        SELECT  DISTINCT ts                AS start_time,
                                EXTRACT(hour from ts)      AS hour,
                                EXTRACT(day from ts)       AS day,
                                EXTRACT(week from ts)      AS week, 
                                EXTRACT(month from ts)     AS month,
                                EXTRACT(year from ts)      AS year,
                                EXTRACT(dayofweek from ts) AS weekday
                        FROM staging_events;
                    """)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
