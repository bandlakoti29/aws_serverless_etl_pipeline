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
