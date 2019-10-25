import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES


staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(artist varchar,auth varchar,firstName varchar,gender varchar,
itemInSession varchar,lastName varchar,length numeric(10,6),level varchar, location varchar, method varchar,page varchar,
registration varchar,sessionid varchar,song varchar,status int,ts bigint,
userAgent varchar,userId bigint)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(song_id varchar,title varchar, year int,
 duration numeric(10,6),artist_id varchar,artist_name varchar, artist_location varchar, artist_latitude varchar, artist_longitude varchar)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id bigint IDENTITY(0,1) PRIMARY KEY,start_time bigint,user_id bigint,
level varchar,song_id varchar,artist_id varchar,session_id varchar,location varchar,user_agent varchar,
CONSTRAINT u_constraint UNIQUE(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent));
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id varchar PRIMARY KEY,first_name varchar,last_name varchar,
gender varchar,level varchar)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY,title varchar,artist_id varchar,year int,
duration numeric(10,6))
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY,name varchar,location varchar,latitude varchar,
longitude varchar) 
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time bigint PRIMARY KEY,hour int,day int,week int,month int, year int,weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from 's3://udacity-dend/log_data'
credentials 'aws_iam_role={}' 
compupdate off region 'us-west-2'
JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role={}'
compupdate off region 'us-west-2'
JSON 'auto' truncatecolumns;
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select ts,userId,level,song_id,artist_id,sessionId,location,userAgent 
FROM
( select events.ts,events.userId,events.level,sa.song_id,sa.artist_id,events.sessionId,events.location,events.userAgent
FROM
(select ts,userId,level,song,artist,sessionId,location,userAgent,length from (select ts,userId,level,song,artist,sessionId,location,userAgent, 
length, page, ROW_NUMBER() OVER(PARTITION BY ts,page,userId,sessionId,level,location order by ts) as events_ranked from staging_events) as ranked
where ranked.events_ranked=1 and ranked.page='NextSong') as events
JOIN 
(SELECT songs.song_id, artists.artist_id, songs.title, artists.name,songs.duration

FROM songs

JOIN artists

ON songs.artist_id = artists.artist_id) AS sa

ON (sa.title = events.song

AND sa.name = events.artist

AND sa.duration = events.length)) as details
""")

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name,gender,level)
select userId,firstname,lastname,gender,level from (select userId,firstname,lastname,gender,level,page,
ROW_NUMBER() OVER (PARTITION BY userId,page order by userId) as user_id_ranked from staging_events) as ranked
where ranked.user_id_ranked=1 and ranked.page='NextSong' and userId is not null
""")

song_table_insert = ("""INSERT INTO songs(song_id,title,artist_id,year,duration) 
select song_id,title,artist_id,year,duration from (select song_id,title,artist_id,year,duration,
ROW_NUMBER() OVER (PARTITION BY song_id order by song_id) as song_id_ranked from staging_songs) as ranked
where ranked.song_id_ranked=1 and song_id is not null
""")

artist_table_insert = ("""INSERT INTO artists(artist_id,name,location,latitude,longitude)
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
from (select artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
ROW_NUMBER() OVER (PARTITION BY artist_id order by artist_id) as artist_id_ranked from staging_songs) as ranked
where ranked.artist_id_ranked=1 and artist_id is not null
""")

time_table_insert = ("""INSERT into time(start_time,hour,day,week,month,year,weekday)
SELECT ts, extract(hour from start_time) as hour,
 extract(day from start_time) as day,extract(week from start_time) as week, 
 extract(month from start_time) as month,  extract(year from start_time) as year,
 extract(dow from start_time) as weekday
 FROM (select distinct(ts) ,TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time  from staging_events where page='NextSong')
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert, time_table_insert,user_table_insert,songplay_table_insert]
