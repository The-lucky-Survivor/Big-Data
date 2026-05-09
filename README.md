# Big Data Hotel Booking Pipeline

End-to-end Big Data pipeline for processing hotel booking data using Airflow, HDFS, Spark, and Snowflake.

## Architecture

Pipeline Flow:

1. Raw CSV file is stored locally.
2. Airflow uploads the CSV to HDFS.
3. PySpark performs ETL and writes cleaned Parquet files to HDFS.
4. Airflow loads the processed data into Snowflake.
5. Validation queries are executed on the final warehouse table.

## Tech Stack

- Apache Airflow
- Apache Spark (PySpark)
- HDFS
- Snowflake
- Docker & Docker Compose
- Python
- Pandas

## Project Structure

```text
Big-Data/
├── dags/
│   └── hotel_final_pipeline.py
├── scripts/
│   ├── etl_hotel.py
│   └── load_to_dwh.py
├── data/
│   └── hotel_bookings_processed.csv
├── config/
├── jars/
│   └── postgresql-42.5.1.jar
├── notebooks/
├── docker-compose.yaml
├── Dockerfile.airflow
├── Architecture_Diagram.drawio.pdf
├── DWH_Schema.jpg
├── README.md
└── .gitignore
