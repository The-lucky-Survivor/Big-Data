from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, round as spark_round
from pyspark.sql.types import DoubleType

def main():
    spark = SparkSession.builder \
        .appName("HotelBookingETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # قراءة البيانات من HDFS
    input_path = "hdfs://hadoop-namenode:9000/data/hotel_bookings.csv"
    print(f"Reading data from: {input_path}")
    
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    
    print(f"Total raw records: {df.count()}")
    
    # إزالة التكرارات
    df = df.dropDuplicates()
    
    # تحويل الأعمدة الرقمية
    numeric_cols = ["lead_time", "stays_in_weekend_nights", "stays_in_week_nights", 
                    "adults", "children", "babies", "adr", "total_of_special_requests"]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
    
    # تحويل التاريخ
    if "reservation_status_date" in df.columns:
        df = df.withColumn("reservation_status_date", to_date(col("reservation_status_date"), "yyyy-MM-dd"))
        df = df.filter(col("reservation_status_date").isNotNull())
    
    # إضافة عمود total_stay_nights
    if "stays_in_weekend_nights" in df.columns and "stays_in_week_nights" in df.columns:
        df = df.withColumn("total_stay_nights", col("stays_in_weekend_nights") + col("stays_in_week_nights"))
    
    # إضافة عمود total_revenue
    if "adr" in df.columns and "total_stay_nights" in df.columns:
        df = df.withColumn("total_revenue", spark_round(col("adr") * col("total_stay_nights"), 2))
    
    # تحديد الأعمدة النهائية
    final_columns = ["hotel", "is_canceled", "lead_time", "arrival_date_year", "arrival_date_month",
                     "stays_in_weekend_nights", "stays_in_week_nights", "total_stay_nights",
                     "adults", "children", "babies", "country", "market_segment", 
                     "distribution_channel", "adr", "total_revenue", "reservation_status",
                     "reservation_status_date"]
    
    available_cols = [c for c in final_columns if c in df.columns]
    df_final = df.select(available_cols)
    
    # حفظ النتيجة بصيغة Parquet
    output_path = "hdfs://hadoop-namenode:9000/data/hotel_processed"
    df_final.write.mode("overwrite").parquet(output_path)
    
    print(f"✅ Processed records saved to {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()
