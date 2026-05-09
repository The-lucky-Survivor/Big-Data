# scripts/load_to_dwh.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LoadToDWH") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.5.1.jar") \
    .getOrCreate()

# اقرأ parquet من HDFS
df = spark.read.parquet("hdfs://hadoop-namenode:9000/data/hotel_processed")

# حدد الأعمدة المطلوبة للـ DWH
columns_to_load = ["hotel", "is_canceled", "lead_time", "arrival_date_year", "arrival_date_month",
                   "arrival_month", "season", "total_stay_nights", "adr", "total_revenue",
                   "country", "market_segment", "reservation_status", "reservation_status_date"]
df_dwh = df.select(*[c for c in columns_to_load if c in df.columns])

# كتابة إلى PostgreSQL (تأكد من وجود jar driver)
url = "jdbc:postgresql://postgres_airflow:5432/airflow"
properties = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}
df_dwh.write.mode("overwrite").jdbc(url, "hotel_fact", properties=properties)

spark.stop()
