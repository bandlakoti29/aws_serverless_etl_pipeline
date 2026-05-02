Project Overview

This project demonstrates the design and implementation of a fully automated, serverless ETL pipeline using AWS services. The pipeline is built to handle real-time data ingestion, schema detection, transformation, data quality validation, and monitoring without any manual intervention.

Whenever new data is uploaded into an S3 bucket, the system automatically triggers downstream processes, making the entire workflow event-driven and scalable.

Architecture & Workflow

The pipeline starts when a new file is uploaded into an S3 bucket. This event triggers an AWS Lambda function, which acts as the orchestrator of the pipeline. The Lambda function performs two key tasks: it triggers an AWS Glue Crawler to detect the schema and update the Glue Data Catalog, and it also initiates an AWS Glue ETL job to process the data.

Inside the ETL job, data is transformed using PySpark. The transformations include schema mapping, duplicate removal, handling missing values, and standardizing inconsistent categorical values. For example, missing numerical values are filled using mean imputation, while categorical gaps such as outlet size are filled using pattern-based logic derived from location type and outlet type.

Additionally, feature-level cleaning is applied, such as scaling visibility values and normalizing categorical fields like fat content.

Before storing the final output, AWS Glue Data Quality checks are applied to ensure the reliability of the dataset. These checks validate completeness, uniqueness, and overall schema integrity.

The cleaned and validated data is then written to:

Amazon S3 in partitioned Parquet format
AWS Glue Data Catalog for downstream analytics


Event Monitoring & Alerts

The pipeline also includes a monitoring and alerting mechanism. If any object is deleted from the S3 bucket, an event is triggered that sends a notification via Amazon SNS. This notification is delivered as an email and includes details about the affected bucket.

The entire pipeline is monitored using Amazon CloudWatch, which captures logs, tracks job execution, and helps in debugging failures.

🧠 Key Features
Fully serverless and event-driven architecture
Automatic schema detection using Glue Crawler
PySpark-based ETL transformations in AWS Glue
Data quality validation using AWS Glue Data Quality
Partitioned data storage for optimized querying
Real-time alerts using SNS
End-to-end monitoring using CloudWatch
🛠️ Tech Stack

AWS Glue (ETL + Crawler + Data Quality)
AWS Lambda (Orchestration)
Amazon S3 (Storage & Event Trigger)
Amazon SNS (Notifications)
Amazon CloudWatch (Monitoring)
PySpark (Data Processing)


Challenges Faced

One of the main challenges in this project was handling schema inconsistencies and missing values efficiently in a distributed environment. Implementing pattern-based imputation for columns like outlet size required grouping and window functions, which introduced complexity in PySpark.

Another challenge was managing Glue DynamicFrames and Spark DataFrames, especially during transformations and joins, where issues like ambiguous column references occurred.

Structuring the pipeline in a modular and scalable way, while ensuring smooth integration between multiple AWS services, was also a key learning experience.

📈 Learning Outcomes

This project helped me gain hands-on experience in building real-world data engineering pipelines using AWS. It improved my understanding of distributed data processing, event-driven architectures, data cleaning techniques, and implementing data quality checks in production-grade systems.
