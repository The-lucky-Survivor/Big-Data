from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import subprocess
import tempfile

default_args = {
    'owner': 'seif',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'hotel_final_pipeline',
    default_args=default_args,
    description='Complete hotel ETL pipeline with Snowflake',
    schedule_interval=None,
    catchup=False,
    tags=['hotel', 'snowflake'],
)

# 1. Create HDFS directory
prepare_hdfs = BashOperator(
    task_id='prepare_hdfs',
    bash_command='docker exec hadoop-namenode bash -c "hdfs dfs -mkdir -p /data || true"',
    dag=dag,
)

# 2. Upload CSV to HDFS
upload_csv = BashOperator(
    task_id='upload_csv_to_hdfs',
    bash_command='docker cp /opt/airflow/data/hotel_bookings_processed.csv hadoop-namenode:/hotel_bookings.csv && docker exec hadoop-namenode bash -c "hdfs dfs -put -f /hotel_bookings.csv /data/"',
    dag=dag,
)

# 3. Run Spark ETL
run_spark_etl = BashOperator(
    task_id='run_spark_etl',
    bash_command='docker exec spark-jupyter bash -c "spark-submit --master local[*] /home/jovyan/etl_hotel.py"',
    dag=dag,
)

# 4. Verify ETL output
verify_output = BashOperator(
    task_id='verify_hdfs_output',
    bash_command='docker exec hadoop-namenode bash -c "hdfs dfs -ls /data/hotel_processed && echo ETL completed successfully"',
    dag=dag,
)

# 5. Load data from HDFS to Snowflake
def load_parquet_to_snowflake(**context):
    import os
    import shutil
    import tempfile
    import subprocess
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    # إنشاء فولدر مؤقت داخل الـ Airflow container
    local_tmp = tempfile.mkdtemp()

    try:
        print("Copying parquet files from HDFS...")

        # نسخ ملفات parquet من HDFS إلى داخل namenode
        subprocess.run([
            "docker", "exec", "hadoop-namenode",
            "bash", "-c",
            "hdfs dfs -copyToLocal -f /data/hotel_processed/* /tmp/"
        ], check=True)

        # نسخ الملفات من namenode إلى airflow-webserver
        subprocess.run([
            "docker", "cp",
            "hadoop-namenode:/tmp/",
            local_tmp
        ], check=True)

        # البحث عن ملفات parquet
        parquet_files = []
        for root, dirs, files in os.walk(local_tmp):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, file))

        if not parquet_files:
            raise Exception("No parquet files found.")

        print(f"Found {len(parquet_files)} parquet files")

        # قراءة جميع الملفات ودمجها
        dfs = [pd.read_parquet(f) for f in parquet_files]
        df = pd.concat(dfs, ignore_index=True)

        print(f"Loaded {len(df)} rows")

        # الاتصال بـ Snowflake
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        cursor = conn.cursor()

        # إنشاء الجدول
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS HOTEL_FACT (
            hotel VARCHAR,
            is_canceled INTEGER,
            lead_time FLOAT,
            arrival_date_year INTEGER,
            arrival_date_month VARCHAR,
            stays_in_weekend_nights FLOAT,
            stays_in_week_nights FLOAT,
            total_stay_nights FLOAT,
            adults FLOAT,
            children FLOAT,
            babies FLOAT,
            country VARCHAR,
            market_segment VARCHAR,
            distribution_channel VARCHAR,
            adr FLOAT,
            total_revenue FLOAT,
            reservation_status VARCHAR,
            reservation_status_date DATE
        )
        """)

        # تنظيف الجدول
        cursor.execute("TRUNCATE TABLE HOTEL_FACT")

        # تحويل أسماء الأعمدة إلى Uppercase
        df.columns = [c.upper() for c in df.columns]

        # رفع البيانات
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            "HOTEL_FACT",
            schema="PUBLIC"
         )

        if not success:
            raise Exception("write_pandas failed")

        print(f"✅ Uploaded {nrows} rows in {nchunks} chunks")

        cursor.close()
        conn.close()

    finally:
        shutil.rmtree(local_tmp, ignore_errors=True)

load_to_snowflake = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_parquet_to_snowflake,
    dag=dag,
)

# 6. Final validation
validate_snowflake = SnowflakeOperator(
    task_id='validate_snowflake',
    snowflake_conn_id='snowflake_default',
    sql="""
    SELECT 
        'Validation Results' as check_type,
        COUNT(*) as total_rows,
        COUNT(DISTINCT hotel) as unique_hotels,
        ROUND(AVG(adr), 2) as avg_daily_rate,
        ROUND(SUM(total_revenue), 2) as total_revenue
    FROM hotel_fact;
    """,
    dag=dag,
)

# Set task order
prepare_hdfs >> upload_csv >> run_spark_etl >> verify_output >> load_to_snowflake >> validate_snowflake
