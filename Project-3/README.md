# Data Engineer with AWS

Projects for Data Engineer with AWS course on Udacity

# Project Details

    The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

        * trains the user to do a STEDI balance exercise;
        * and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
        * has a companion mobile app that collects customer data and interacts with the device sensors.

    STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

    Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

    The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

    Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.


# Data Validation

    Checking the number of rows of landing data inside s3

    !(./images/data_validation.png)

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
