# 📘 Complete Project Guide — Big Data Hotel Booking Pipeline

> A comprehensive, step-by-step guide to understand, set up, and run the entire project on **Ubuntu**.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [How the Pipeline Works](#2-how-the-pipeline-works)
3. [Ubuntu Setup Guide (From Zero)](#3-ubuntu-setup-guide-from-zero)
4. [Running the Pipeline](#4-running-the-pipeline)
5. [File-by-File Explanation](#5-file-by-file-explanation)
6. [Data Warehouse Schema Explained](#6-data-warehouse-schema-explained)
7. [Docker Infrastructure Deep Dive](#7-docker-infrastructure-deep-dive)
8. [Common Questions a Professor Might Ask](#8-common-questions-a-professor-might-ask)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Project Overview

### What is this project?

This is an **end-to-end Big Data ETL pipeline** that:
- Takes raw hotel booking data (CSV file with ~120K records)
- Processes it through a distributed computing stack
- Loads the clean data into a cloud data warehouse (Snowflake)

### Why is it "Big Data"?

Even though the dataset is ~11MB, the infrastructure is designed for **horizontal scalability**:
- **HDFS** with 2 DataNodes — can handle petabytes of data
- **Spark** with distributed execution — processes data in parallel
- **YARN** for resource management — allocates compute across the cluster
- **Airflow** for orchestration — schedules and monitors complex workflows

### The Big Picture

```
Raw CSV → HDFS → Spark ETL → Parquet (HDFS) → Snowflake → Validation
              ↑                                     ↑
              └──────── Apache Airflow ──────────────┘
                      (Controls everything)
```

---

## 2. How the Pipeline Works

### Stage 1: Data Ingestion (CSV → HDFS)

**What happens:** The raw CSV file is uploaded from the local filesystem into HDFS (Hadoop Distributed File System).

**Why HDFS?** In a real production environment, data could be terabytes in size. HDFS splits files into 128MB blocks and distributes them across multiple machines for parallel processing.

**Technical flow:**
```
1. Airflow creates /data directory in HDFS
2. docker cp copies CSV from Airflow container → Hadoop NameNode container
3. hdfs dfs -put uploads the file into HDFS
```

### Stage 2: ETL with PySpark

**What happens:** Apache Spark reads the CSV from HDFS, cleans it, transforms it, and writes the result as Parquet files back to HDFS.

**ETL Steps:**
| Step | Operation | Why |
|------|-----------|-----|
| 1 | Read CSV from HDFS | Load data into Spark DataFrame |
| 2 | Drop duplicates | Remove identical rows |
| 3 | Cast numeric columns | Ensure proper data types (String → Double) |
| 4 | Parse dates | Convert date strings to Date type |
| 5 | Create `total_stay_nights` | Feature engineering: weekend + weekday nights |
| 6 | Create `total_revenue` | Business metric: ADR × total nights |
| 7 | Select final columns | Keep only 18 relevant columns |
| 8 | Write Parquet to HDFS | Compressed columnar format for efficiency |

**Why Parquet?** It's a columnar storage format that is:
- 10x smaller than CSV (compression)
- 100x faster for analytical queries (column pruning)
- The standard format for data lakes

### Stage 3: Load to Snowflake

**What happens:** Airflow reads the Parquet files from HDFS, converts them to a Pandas DataFrame, and bulk-loads them into a Snowflake table using `write_pandas()`.

**Technical flow:**
```
1. Copy Parquet from HDFS → NameNode local filesystem
2. Copy from NameNode → Airflow container
3. Read Parquet files with Pandas
4. Connect to Snowflake via snowflake-connector-python
5. CREATE TABLE IF NOT EXISTS HOTEL_FACT
6. TRUNCATE TABLE (clear old data)
7. write_pandas() — bulk upload
```

### Stage 4: Validation

**What happens:** A SQL query runs on Snowflake to verify the data was loaded correctly:
```sql
SELECT COUNT(*) as total_rows,
       COUNT(DISTINCT hotel) as unique_hotels,
       AVG(adr) as avg_daily_rate,
       SUM(total_revenue) as total_revenue
FROM hotel_fact;
```

---

## 3. Ubuntu Setup Guide (From Zero)

### 3.1 System Requirements

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| RAM | 8 GB | 16 GB |
| Disk | 20 GB free | 40 GB free |
| CPU | 2 cores | 4+ cores |
| Ubuntu | 20.04 LTS | 22.04 LTS |

### 3.2 Install Docker

```bash
# 1. Update system
sudo apt update && sudo apt upgrade -y

# 2. Install prerequisites
sudo apt install -y ca-certificates curl gnupg lsb-release

# 3. Add Docker GPG key
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

# 4. Add Docker repository
echo "deb [arch=$(dpkg --print-architecture) \
  signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 5. Install Docker Engine + Compose
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# 6. Run Docker without sudo
sudo usermod -aG docker $USER
newgrp docker

# 7. Verify
docker --version          # Should show 20.10+
docker compose version    # Should show v2.0+
```

### 3.3 Install Git

```bash
sudo apt install -y git
git --version
```

### 3.4 Clone & Setup

```bash
# Clone the repository
git clone https://github.com/The-lucky-Survivor/Big-Data.git
cd Big-Data

# Create necessary directories
mkdir -p ./logs ./plugins ./config ./output ./notebooks

# Set Airflow UID for file permissions
echo "AIRFLOW_UID=$(id -u)" > .env
```

### 3.5 Build & Start Everything

```bash
# Build and start all 12 containers
docker compose up -d --build

# Wait 5-10 minutes for first-time image downloads
# Monitor progress:
docker compose logs -f
# Press Ctrl+C to stop following logs
```

### 3.6 Verify Services

```bash
# Check all containers are running
docker compose ps

# Expected: all containers should be "Up" or "healthy"
```

### 3.7 Access Web Interfaces

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:18080 | airflow / airflow |
| Jupyter | http://localhost:8899 | No password |
| HDFS NameNode | http://localhost:9870 | — |
| YARN ResourceManager | http://localhost:8088 | — |

### 3.8 Configure Snowflake Connection

1. Open http://localhost:18080 → Login
2. Go to **Admin → Connections → Add (+)**
3. Set Connection Id = `snowflake_default`
4. Fill in your Snowflake credentials
5. Save

### 3.9 Run the Pipeline

1. In Airflow UI → find `hotel_final_pipeline`
2. Toggle it **ON**
3. Click **▶ Trigger DAG**
4. Watch the Graph View for progress

---

## 5. File-by-File Explanation

### 📄 `dags/hotel_final_pipeline.py`

**Purpose:** The main Airflow DAG — the "brain" of the pipeline.

**What is a DAG?** A Directed Acyclic Graph — a collection of tasks with defined execution order. No task can create a cycle (A→B→C, never C→A).

**Key components:**

```python
# Default configuration for all tasks
default_args = {
    'owner': 'seif',              # Who owns this pipeline
    'retries': 1,                 # Retry failed tasks once
    'retry_delay': timedelta(minutes=2),  # Wait 2 min before retry
}

# The DAG itself
dag = DAG(
    'hotel_final_pipeline',       # Unique identifier
    schedule_interval=None,       # Manual trigger only (no auto-schedule)
    catchup=False,                # Don't run for past dates
)
```

**The 6 tasks:**

| Task | Type | What it does |
|------|------|--------------|
| `prepare_hdfs` | BashOperator | Creates `/data` folder in HDFS |
| `upload_csv_to_hdfs` | BashOperator | Copies CSV to HDFS via `docker cp` + `hdfs dfs -put` |
| `run_spark_etl` | BashOperator | Runs `spark-submit` inside the Jupyter container |
| `verify_hdfs_output` | BashOperator | Lists HDFS output to confirm ETL worked |
| `load_to_snowflake` | PythonOperator | Python function that reads Parquet and uploads to Snowflake |
| `validate_snowflake` | SnowflakeOperator | Runs a validation SQL query |

**Important:** This DAG uses `docker exec` to control other containers. This works because the Docker socket (`/var/run/docker.sock`) is mounted into the Airflow containers.

---

### 📄 `scripts/etl_hotel.py`

**Purpose:** The PySpark ETL script that transforms raw data.

**Line-by-line explanation:**

```python
# Create a Spark session (entry point to Spark)
spark = SparkSession.builder \
    .appName("HotelBookingETL") \
    .config("spark.sql.adaptive.enabled", "true") \  # Adaptive Query Execution
    .getOrCreate()

# Read CSV from HDFS — Spark connects to NameNode at port 9000
df = spark.read.option("header", True).option("inferSchema", True) \
    .csv("hdfs://hadoop-namenode:9000/data/hotel_bookings.csv")

# Remove duplicate rows
df = df.dropDuplicates()

# Cast string columns to numeric (DoubleType)
for c in numeric_cols:
    df = df.withColumn(c, col(c).cast(DoubleType()))

# Create new calculated columns
df = df.withColumn("total_stay_nights",
    col("stays_in_weekend_nights") + col("stays_in_week_nights"))

df = df.withColumn("total_revenue",
    spark_round(col("adr") * col("total_stay_nights"), 2))

# Save as Parquet (columnar format, compressed)
df_final.write.mode("overwrite").parquet(output_path)
```

**Key Spark concepts used:**
- `SparkSession` — Entry point to all Spark functionality
- `DataFrame` — Distributed collection of data organized into columns
- `withColumn()` — Adds or replaces a column
- `cast()` — Converts data types
- `mode("overwrite")` — Replace existing data on write

---

### 📄 `scripts/load_to_dwh.py`

**Purpose:** Alternative loader — writes Parquet data to PostgreSQL instead of Snowflake.

```python
# Read processed Parquet from HDFS
df = spark.read.parquet("hdfs://hadoop-namenode:9000/data/hotel_processed")

# Write to PostgreSQL using JDBC
df_dwh.write.mode("overwrite").jdbc(url, "hotel_fact", properties=properties)
```

**When to use this?** If Snowflake is not available and you want to demo with PostgreSQL as the data warehouse.

---

### 🐳 `docker-compose.yaml`

**Purpose:** Defines the entire infrastructure as code.

**Key concepts:**

```yaml
# YAML anchor — reusable configuration block
x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile.airflow   # Custom image with Python packages

# Merge key — inherits all config from the anchor
airflow-webserver:
  <<: *airflow-common               # Inherits all airflow-common settings
  command: webserver                 # But with a specific command
  ports:
    - "18080:8080"                   # Maps host:18080 → container:8080
```

**Volume mounts explained:**
```yaml
volumes:
  - ./dags:/opt/airflow/dags         # DAG files (live reload)
  - ./data:/opt/airflow/data         # CSV data files
  - /var/run/docker.sock:/var/run/docker.sock  # Docker-in-Docker control
```

---

### 🐳 `Dockerfile.airflow`

**Purpose:** Creates a custom Airflow Docker image.

```dockerfile
FROM apache/airflow:2.10.4                    # Base Airflow image
RUN pip install --no-cache-dir \
    pyspark pandas pyarrow \                  # For Spark & data handling
    snowflake-connector-python                # For Snowflake connection
```

---

## 6. Data Warehouse Schema Explained

The project uses a **Star Schema** — the most common DWH design pattern.

### What is a Star Schema?

A central **Fact Table** (with measurements/metrics) surrounded by **Dimension Tables** (with descriptive attributes). It looks like a star.

### Tables:

**FACT_BOOKINGS** (center) — Contains all measurable data:
- `is_canceled`, `lead_time`, `total_stay_nights`, `adr`, `total_revenue`
- Foreign keys linking to each dimension

**DIM_HOTEL** — Hotel type (Resort Hotel / City Hotel)

**DIM_DATE** — Date details (year, month, quarter, day of week)

**DIM_CUSTOMER** — Customer country of origin (USA, FRA, GBR, etc.)

**DIM_MARKET** — How the booking was made (Online TA, Direct, Corporate, etc.)

### Why Star Schema?

- **Simple queries** — JOINs are straightforward
- **Fast aggregations** — Optimized for `GROUP BY` and `SUM/AVG`
- **Easy to understand** — Business users can navigate it intuitively

---

## 7. Docker Infrastructure Deep Dive

### Container Groups

**Group 1: Airflow (5 containers)**
| Container | Role |
|-----------|------|
| `postgres_airflow` | Stores Airflow metadata (DAG runs, task states) |
| `airflow-webserver` | The web UI you interact with |
| `airflow-scheduler` | Decides when to run tasks |
| `airflow-triggerer` | Handles deferred/async tasks |
| `airflow-init` | One-time setup: creates DB, admin user |

**Group 2: Spark (1 container)**
| Container | Role |
|-----------|------|
| `spark-jupyter` | Jupyter Notebook + PySpark engine |

**Group 3: Hadoop (6 containers)**
| Container | Role |
|-----------|------|
| `hadoop-namenode` | Manages HDFS metadata (which blocks are where) |
| `hadoop-datanode1` | Stores actual data blocks (replica 1) |
| `hadoop-datanode2` | Stores actual data blocks (replica 2) |
| `resourcemanager` | YARN — allocates CPU/memory to jobs |
| `hadoop-nodemanager` | YARN worker node 1 |
| `hadoop-nodemanager2` | YARN worker node 2 |

### Network

All containers are on a custom Docker bridge network `sparknet` (subnet `172.30.0.0/16`). Each container has a **static IP** so they can reliably find each other.

---

## 8. Common Questions a Professor Might Ask

### Q: Why did you choose these specific technologies?

**A:** Each tool serves a specific purpose in the Big Data ecosystem:
- **Airflow** — Industry standard for workflow orchestration (used by Airbnb, Spotify)
- **HDFS** — Fault-tolerant distributed storage designed for large datasets
- **Spark** — 100x faster than MapReduce for in-memory processing
- **Snowflake** — Cloud-native DWH with automatic scaling
- **Docker** — Ensures reproducibility across any environment

### Q: What is ETL?

**A:** Extract, Transform, Load:
- **Extract** — Read raw data from CSV
- **Transform** — Clean, deduplicate, cast types, create new features
- **Load** — Write processed data to Snowflake

### Q: Why Parquet instead of CSV?

**A:**
- **Columnar storage** — Only reads needed columns (faster queries)
- **Compression** — 10x smaller file size
- **Schema preservation** — Data types are embedded in the file
- **Spark-native** — Optimized for distributed processing

### Q: Why use Docker for this project?

**A:** Without Docker, you'd need to install Hadoop, Spark, Airflow, and PostgreSQL manually on separate machines. Docker lets you run the entire cluster on a single machine with one command (`docker compose up`).

### Q: How does Airflow communicate with other containers?

**A:** The Docker socket (`/var/run/docker.sock`) is mounted into Airflow containers. This allows Airflow to run `docker exec` commands to control Hadoop and Spark containers directly.

### Q: What is a Star Schema and why did you use it?

**A:** A star schema has a central fact table with numeric measurements (revenue, nights, etc.) connected to dimension tables with descriptive data (hotel name, date, country). It's optimized for OLAP queries like "What was the total revenue per country per month?"

### Q: How would you scale this for real production?

**A:**
- Add more DataNodes to HDFS for storage
- Add more NodeManagers for compute
- Use Spark's cluster mode instead of `local[*]`
- Configure Airflow with CeleryExecutor for parallel task execution
- Use Kubernetes instead of Docker Compose

### Q: What happens if a task fails?

**A:** Airflow automatically retries the failed task once (configured in `default_args`), waits 2 minutes between retries, and marks it as failed if it still doesn't work. You can see the error in the Airflow logs.

### Q: What is `spark-submit`?

**A:** It's the command used to launch Spark applications on a cluster. In this project, `spark-submit --master local[*] etl_hotel.py` runs the ETL script using all available CPU cores.

---

## 9. Troubleshooting

### Services won't start
```bash
# Check if ports are already in use
sudo lsof -i :18080
sudo lsof -i :9870

# Kill conflicting processes or change ports in docker-compose.yaml
```

### Out of memory
```bash
# Check Docker resource usage
docker stats

# Increase Docker memory (edit /etc/docker/daemon.json)
# Or reduce services: comment out datanode2 and nodemanager2
```

### HDFS in safe mode
```bash
docker exec hadoop-namenode hdfs dfsadmin -safemode leave
```

### Reset everything
```bash
docker compose down -v
docker compose up -d --build
```

---

<p align="center"><em>End of Project Guide</em></p>
