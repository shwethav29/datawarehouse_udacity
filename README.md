# SPARKIFY

Sparkify is a music streaming startup. They have been collecting songs metadata and user activity of their app.

Going forward they want to move their processes and data to the cloud. data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Purpose

The purpose of this project is to build the ETL pipeline that extracts the data from S3, stages them in Redshift, and transforms data to the dimensional
tables for their analytics team.

####> Steps to follow.
>>1 Identify the facts and dimension tables for creating star schema. 

>>2 Build a ETL pipeline using python, to transfer data from S3 to Redshift and then transform to the facts and dimensions table.

>>> S3 link for project Data

>>>>Song data: s3://udacity-dend/song_data

>>>>Log data: s3://udacity-dend/log_data

>>>>Log data json path: s3://udacity-dend/log_json_path.json

## Schema
We will be creating a star schema with following facts and dimension tables

###>**Fact table**
In case of Sparkify app, the songs played by the users is the business process that needs to be modelled. The song played is the fact grain.  The ETL pipeline will extract this data from the 
files in the log_data and will populate this table. The primary key for this table is the songplay_id
which is auto generated. 
 >>**1. SONGPLAYS -** log data associated with song play, which is with page='NextPage'.
 >>>>songplay_id a auto_increment id column, start_time bigint,user_id bigint,level varchar,song_id varchar,artist_id varchar,session_id varchar,location varchar,user_agent varchar
 
### > **Dimension tables**
We have recognised 4 dimension tables to capture user data, song data, artist data and to provide a 
time data. The time table helps to slice and dice data based on different time variables.
 >>**1. USERS -** user data from the log_data
 >>>>user_id varchar,first_name varchar,last_name varchar,gender varchar,level varchar

>>**2. ARTISTS -** artists data from song_data directory
 >>>>artist_id varchar,name varchar,location varchar,latitude varchar,longitude varchar

>>**3. SONGS -** song data from the song_data directory
 >>>>song_id varchar,title varchar,artist_id varchar,year int,duration numeric

>>**4. TIME -** time data extracted from the timestamp field of lod_data
 >>>>start_time bigint,hour int,day int,week int,month int, year int,weekday int 

### >**ETL Pipeline**
>>> 1.The data from S3 log_data and song_data is loaded to staging_events and staging_songs on Redshift.
>>> 2. We perform bulk inserts from staging table to the facts and dimensions tables. 
>>> 3. Using ROW_NUMBER OVER PARTITION by the key, fetches first occurrence of the key, in case of duplicates. This gives a better performance.
>>> 4. First we load all dimension table. Followed by reading the artist_id and song_id keys from these dimension table to populate the songplays table.
>>> 5. We are interested only in log_data with page='NextPage'. The timestamp column mapped to the time table by extracting datetime properties for the give timestamp. 
##Project Structure
1. redshift.py for creating the redshift cluster, with roles.
2. probe_redshift.py to fetch host and role arn information
3.All the sql queries are defined in sql_queries.py
4.create_tables.sql creates sparkify schema.
5.etl.py is the ETL pipeline to process the S3 log_data and song_data and populate the tables.
6. delete_redshift_cluster.py to delete the redshift cluster.
7. delete_create_resources for the final clean up to detach and delete the roles.
##Steps

### >**Redshift cluster**
>> 1. Update dwh.cfg with the AWS KEY AND SECRET.
>> 2. Run redshift.py to create the redshift cluster. Make sure the cluster is available.
>> 3. Run probe_redshift.py to get ENDPOINT and THE ROLE_ARN
>> 4. Update dwf.cfg with HOST = ENDPOINT AND ARN = ROLEARN

### >**Perform data load**
1. execute create_tables.py
2. execute etl.py
3. check the analytics queries on query editor of the AWS Redshift.

### >**Clean Up**
1.execute delete_redshift_cluster.py
2.Wait for the cluster to be deleted.
3.execute delete_create_resources.py

## Trouble shoot
In case of any errors or to look for the progress of the etl. Go to the cluster nad click on the query tab.

### sample dwh.cfg
[AWS]
KEY=
SECRET=

[DWH]
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=8
DWH_NODE_TYPE=dc2.large

DWH_IAM_ROLE_NAME=dwhRole
DWH_CLUSTER_IDENTIFIER=samples3parkify

[CLUSTER]
HOST= <<from the ENDPOINT>>
DB_NAME=sparkify
DB_USER=sparkifyuser
DB_PASSWORD=Passw0rd
DB_PORT=5439

[IAM_ROLE]
ARN=<<ROLE_ARN>>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'