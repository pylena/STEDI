# STEDI Human Balance Analytics Project

## Overview
The STEDI Human Balance Analytics Project is designed to build a data lakehouse solution on AWS to support the development of a machine learning model that detects human steps using sensor data. This project leverages AWS Glue, Spark, and other AWS services to process, transform, and curate sensor data from STEDI Step Trainers and mobile apps.

### Project Workflow
![image](https://github.com/user-attachments/assets/145073e7-bf34-4038-b79a-0b1e5d364815)

### Data Source
 - Customer Data: User information from the mobile app.
 - Step Trainer Sensor Data: Distance measurements.
 - Accelerometer Data:  X, Y, Z  motion data.


### Workflow steps
* Landing Zone: Raw Data
 * Trusted Zone: Filter records to include only customers who have approved to share data with research, join related datasets to ensure accuracy.
* Curated Zone : Generate datasets ready for analysis and machine learning.



### Technologies & Tools
* AWS Glue – For ETL, jobs and data processing.
* Amazon S3 – Data lake storage.
* AWS Athena – Query and validate data.



