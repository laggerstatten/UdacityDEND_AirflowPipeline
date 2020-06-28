# BACKGROUND

## Fictional
Sparkify is an online music streaming service, in which users can select songs to listen to and play them. The company records these listening events and has demographic information about their users. Additionally they have information about the music in their collection. The Sparkify team wants to combine information from their listening events log with their song information in order to gain insight into user behavior. This database exists in order to synthesize data from two separate data sets, a log of customer listening events and information describing songs in the library.

This data originally resided on local servers in JSON format, but has since been moved to S3 in the cloud. In order to monitor the ETL process, Apache Airflow will be used to construct the pipeline, and analysis can be performed in Redshift.

## Real
Sparkify is a model of an online music streaming service, similar to Spotify or Pandora. The dataset for the songs comes from the [Million Song Dataset] (http://millionsongdataset.com/) and the dataset for the user behavior was generated using the [Eventsim event simulator] (http://millionsongdataset.com/). The database and ETL processes are implemented using Python, Airflow, S3, and Redshift.


# STRUCTURE & ETL PIPELINE

The final database consists of 5 tables, a fact table (songplays) and four dimension tables (users, songs, artists, and time). These are assembled in a star schema, with each of the four dimension tables providing attribute information for the songplays fact table. Each table reflects a real-world entity within the scope of the Sparkify project. A "songplay" is an event in which a user plays a song on the app. This event necessarily involves a user and a song, and occurs at a time. Additionally, every song must have an artist.

Data about songs and song play events are found in JSON files on S3. These full JSON files are read into staging tables, and contain more data than is used for the Sparkify project. During the ETL process, relevant columns are used to construct a new "songs" table, while other columns are used to construct the "artists" table. This decomposition into two separate tables is part of the normalization process, in order to separately describe the songs and artists entities. Data about the users is extracted to a separate "users" table. Timestamp data is also used to construct a separate "time" table with derived attributes as part of the date heirarchy.

A *song_title* and an *artist_name* field serve as keys to relate the songplays table to the songs and artists tables respectively. During the ETL process, song and artist data are brought into the songplays table when a relationship exists.

![Star Schema ERD](/sparkify_erd.png)


# INSTRUCTIONS

#The user must initiate Airflow by running the "/opt/airflow/start.sh" command. 

#The user must create a Redshift cluster and create connections within Airflow.

#The user must create tables in Redshift either through the Query Editor or the "table_create" DAG.

#The user can then run the "data_pipeline" DAG to stage and load the tables in Redshift.


# CONTENTS

**table_create_dag.py** - this file creates all necessary tables within Redshift
**data_pipeline_dag.py** - this file orchestrates the ETL process including staging and loading
**create_tables.sql** - this file contains the SQL queries to create new tables
**sql_queries.py** - this file contains the SQL queries to insert data into the tables
**stage_redshift.py** - this file stages the JSON data from S3 to Redshift
**load_dimension.py** - this file loads data into the song, artist, user, and time tables
**load_fact.py** - this file loads data into the songplays table
**data_quality.py** - this file performs data quality checks on the data after loading to Redshift
