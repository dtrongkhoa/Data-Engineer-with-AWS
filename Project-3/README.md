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

## Data Validation

    Checking the number of rows of landing data inside s3

![alt text](./images/data_validation.png)

    Query customer_landing and accelerometer_landing tables:

![alt text](./images/customer_landing.png)
![alt text](./images/accelerometer_landing.png)

## Landing to Trusted Zone

    customer_trusted: filter customers that accept their data to be used for research.
    accelerometer_trusted: only store accelerometer data from customer who agreed to share data for research purposes.

Query Customer Trusted Zone

![alt text](./images/customer_trusted_all.png)
![alt text](./images/customer_trusted_validated.png)

## Trusted to Curated Zone

    customer_curated: only customers that have accelerometer data and agreed their data to be used for research purpose.
    step_trainer_curated - machine_learning-curated: an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data

## All Tables

![alt text](./images/all_tables.png)
