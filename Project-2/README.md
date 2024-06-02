# Data Engineer with AWS

Projects for Data Engineer with AWS course on Udacity

# The purpose of this database in context of Sparkify

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Step to achieve the project

    1. Create IAM Role
    * Create dwhRole role with the AmazonS3ReadOnlyAccess permission policy attached

    2. Create Security Group for Redshift
    * Create a redshift_security_group

    3. Create an IAM User for Redshift
    * Create an IAM User with AmazonRedshiftFullAccess and AmazonS3ReadOnlyAccess

    4. Create a RedShift Cluster
    * Create the redshift-cluster-1

    5. Implement ETL Pipeline and SQL_Queries
    * sql_queries.py includes all SQL queries used in create_tables.py and elt.py
    * Update dwh.cfg with correct credentials

    6. Run the project
    * Firstly, run create_tables.py to create staging, fact and dimension table schema using : python create_tables.py
    * Secondly, run elt.py to excecute the ETL process by command: python etl.py

# Database schema

## Staging

    staging_events (
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

    staging_songs (
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

## Fact table

    songplay (
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

## Dimension tables

    user (
    user_id             varchar PRIMARY KEY,
    first_name          varchar,
    last_name           varchar,
    gender              char(1),
    level               varchar
    );

    song (
    song_id             varchar PRIMARY KEY,
    title               varchar,
    artist_id           varchar NOT NULL,
    year                int,
    duration            float
    );

    artist(
    artist_id           varchar PRIMARY KEY,
    name                varchar,
    location            text,
    latitude            float,
    longitude           float
    );

    time(
    start_time          timestamp PRIMARY KEY,
    hour                int,
    day                 int,
    week                int,
    month               int,
    year                int,
    weekday             int
    );

# Notes:

    1. To run on local machine, remember to create virtual environment
    2. Activate virtual environment, install psycopg2  using command: pip install psycopg2
