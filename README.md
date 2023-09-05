# OpenWeatherETL

# Weather ETL Pipeline using Apache Airflow.

# Project Description
This project focuses on building an ETL (Extract, Transform, Load) pipeline using Apache Airflow to automate the retrieval, transformation, and loading of current weather data from the OpenWeatherMap API into an AWS S3 bucket. It streamlines the process of gathering weather information, making it readily available for analysis and other applications.

# Architecture Overview
The architecture of this project revolves around Apache Airflow, a powerful orchestration tool, and leverages OpenWeatherMap API for data extraction. It further employs AWS S3 for storing the transformed data. This ETL pipeline ensures data consistency, accuracy, and accessibility.

# Architecture Diagram

The architecture diagram depicts the flow of data through the ETL pipeline, starting with data extraction from OpenWeatherMap, followed by transformation, and concluding with data loading into an AWS S3 bucket.

# Overview of the Architecture

- Data Extraction: The project initiates by checking the readiness of the OpenWeatherMap API using the is_weather_api_ready task. Once the API is ready, the extract_weather_data task retrieves the current weather data.

- Data Transformation: The transform_load_weather_data task transforms the raw data into a structured format. It calculates temperature in Fahrenheit and adjusts timestamps for local time zones.

- Data Loading: The transformed data is then loaded into an AWS S3 bucket. The pipeline ensures secure and scalable storage of weather data for future analysis.

# Workflow
The workflow of this project is as follows:

1. The Apache Airflow DAG (weather_dag) is triggered on a predefined schedule.

2. The is_weather_api_ready task checks API readiness.

3. Upon successful API readiness, the extract_weather_data task retrieves weather data.

4. The transform_load_weather_data task transforms and loads the data into an AWS S3 bucket.

5. The ETL pipeline ensures the data is consistently updated for analysis.

# Technology Used
- Apache Airflow
- Python
- OpenWeatherMap API
- AWS S3

# Conclusion
This project showcases an efficient ETL pipeline that automates the collection, transformation, and storage of weather data. It ensures data consistency and accessibility, making it a valuable asset for various weather-related applications and analyses. Feel free to contribute or adapt this project to your specific needs.
