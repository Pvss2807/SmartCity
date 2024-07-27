# Project Overview


This project simulates a smart city environment, generating and processing real-time data for vehicles, GPS, traffic cameras, weather conditions, and emergency incidents. The simulation aims to demonstrate a comprehensive data engineering pipeline using Kafka for data streaming, Spark for data processing, and AWS S3 for data storage.

# Technologies Used

Kafka: For real-time data streaming.

Spark: For real-time data processing and analytics.

AWS S3: For data storage.

AWS Glue: For Cataloging the data.

AWS Athena: For data Querying.

AWS Redshift: For schema management and querying capabilities.

Python: For data generation and processing scripts.

Docker: For containerization of the environment.


# Project Components

Data Generation (main.py):



Simulates the movement of a vehicle from Waterloo to Toronto.
Generates real-time data for:
Vehicle telemetry.
GPS coordinates.
Traffic camera snapshots.
Weather conditions.
Emergency incidents.
Sends the generated data to Kafka topics.


Data Streaming and Processing (spark-city.py):


Reads data from Kafka topics using Spark Structured Streaming.
Defines schemas for different data types (vehicle, GPS, traffic, weather, emergency).
Writes processed data to AWS S3 in Parquet format.


Configuration (config.py):

Stores AWS credentials required for accessing S3 buckets.


Docker Compose (docker-compose.yml):

Defines services for Kafka, Spark master, and Spark workers.
Configures Zookeeper and Kafka broker for managing and distributing data streams.


# Setup and Execution

Pre-requisites:

Docker and Docker Compose installed.
AWS S3 bucket and credentials set up.


Running the Project:

Clone the repository.
Navigate to the project directory.
Update AWS credentials in config.py.

Start the Docker services:

bash
Copy code
docker-compose up

Run the data generation script:

bash
Copy code
python main.py
The Spark jobs will automatically start and process the data, writing it to the specified S3 bucket.

# Detailed Explanation of Scripts

main.py:

Environment Variables: Configures Kafka server details and topics.

Data Generation Functions: generate_vehicle_data(), generate_gps_data(), generate_traffic_camera_data(), generate_weather_data(), generate_emergency_incidents_data(). Each function generates realistic data points with unique IDs, timestamps, and random values for attributes.

Kafka Producer: Sends generated data to corresponding Kafka topics.

spark-city.py:

Spark Session Configuration: Sets up Spark with necessary packages and AWS credentials.

Schema Definitions: Defines data schemas for vehicle, GPS, traffic, weather, and emergency data.

Kafka Data Reading: Reads data from Kafka topics and parses JSON messages into structured DataFrames.

Stream Writers: Writes the processed data to AWS S3 in Parquet format.

config.py:

Stores AWS credentials required for accessing S3.

docker-compose.yml:

Configures and manages the Docker containers for Kafka, Spark master, and Spark workers.
Ensures proper networking and service dependencies.
