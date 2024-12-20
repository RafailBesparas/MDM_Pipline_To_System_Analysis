# Project From - https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/
# MDM_Pipline_To_System_Analysis
This project involves developing a comprehensive Data Engineering Pipeline for a Banking Master Data Management (MDM) Centralized Management System. The objective is to extract data from the MDM system utilizing a Hadoop-clustered environment for efficient processing. The pipeline will transform and output the data in JSON format, making it suitable for integration with downstream Analytics and Prediction systems, thereby enabling advanced data-driven decision-making and insights.

# Identified Problem in the Project:
The bank faces a challenge with multiple Master Data Management (MDM) systems requiring consistent copies of entity data. The current process lacks an efficient mechanism to deliver this data to downstream systems without overloading the MDM platform.

# Proposed Solution:
A Kafka-based system is proposed as an intermediary between the MDM platform and the downstream systems to address this issue. The MDM entity data will be ingested into Kafka, enabling downstream systems to consume the data directly from Kafka. This design enhances scalability, reduces the load on the MDM platform, and ensures efficient data distribution.

The pipeline will initially be configured to transfer data from the MDM platform to Kafka on a daily batch schedule. As the system proves effective, the frequency can be increased to hourly transfers, depending on business needs. The pipeline's flexibility allows for weekly, daily, or hourly scheduling data loads, aligning with dynamic business requirements while maintaining system performance and reliability.

# MDM Pipeline Details:
## Entities: 500+ entities are handled by the system.
## Data Export Process:
The MDM platform exports all entity data to the Hadoop platform daily as a full data load.
Exported data is retained for 7 days, and daily export reports are maintained.
## Data Availability:
Entity data is stored in Hive tables, partitioned by the load_date.
Data for any load_date is accessible from these Hive tables.

# Scope of the Project:
This project aims to develop a Spark-based application to process and transmit the MDM entity data from the existing data ingestion pipeline to a Kafka topic. The key deliverables include:

1. Input Handling:
The application must take load_date as an input argument to identify the specific data to process.

2. Data Processing:
Read the entity data for the provided load_date from Hive tables.
Process and transform the data according to the defined business requirements.

3. Data Transmission:
Publish the processed entity data to the appropriate Kafka topic for downstream consumption.

4. Code Development Standards:
Employ modular design for maintainability and scalability.
Emphasize code reuse and adhere to industry best practices.
Implement unit testing to ensure reliability and performance.

5. Resource Estimation:
Assess computational requirements, including:
Number of executors
CPU cores
Memory allocation

6. CI/CD Integration:
Collaborate with the DevOps team to define and implement a robust CI/CD pipeline for automated deployment and testing.

# Creating three branches—Master (Production), Release (QA Testing), and Dev (Development)—is a common practice in software development for maintaining a structured workflow. Here's why each branch is important:

# Master (Production Environment):
This is the stable branch that reflects the code deployed to production.
Only thoroughly tested and approved changes are merged into this branch.
Ensures that the production environment is always in a deployable state.

# Release (QA Testing Environment): 
This branch is used for quality assurance (QA) testing.
Once features in the Dev branch are complete, they are merged into the Release branch for final testing before going live.
Helps isolate testing from ongoing development activities.

# Dev (Development Environment):
This branch serves as the integration point for development work.
Developers merge their feature branches into Dev after completing initial development and testing.
Ensures that the latest development code is consolidated in one place for team collaboration.



