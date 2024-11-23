import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import initcap, trim , lower, col, max , concat_ws, lit, row_number, when, md5, desc
from datetime import datetime, timedelta

# Databases config data
POSTGRES_CONFIG = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "postgres_url": os.getenv("POSTGRES_URL")
}


MYSQL_CONFIG = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "mysql_url": os.getenv("MYSQL_URL")
}


# Minio configurations
MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "lakehouse"


spark = SparkSession.builder \
    .appName("Bronze Job") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS dwh")

def get_last_loaded(table_name):
    parquet_path = f"s3a://{BUCKET_NAME}/bronze/{table_name}/"
    try:
        df = spark.read.parquet(parquet_path)
        last_extraction_time = df.agg(max("extraction_time")).collect()[0][0]
        return last_extraction_time
    except:
        return None      


def extract_mysql_and_save(table_name, snapshot):
    last_loaded = get_last_loaded(table_name)
    jdbc_url = MYSQL_CONFIG["mysql_url"]
    
    
    if last_loaded == None or snapshot:
        query = f"(SELECT *, NOW() AS raw_extraction_time FROM {table_name})"
    else:
        query = f"""
        (
            SELECT *, NOW() AS raw_extraction_time 
            FROM {table_name}
            WHERE created_at > '{last_loaded}' OR updated_at > '{last_loaded}'
        )
        """        
   
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("query", query) \
        .option("user", MYSQL_CONFIG["user"]) \
        .option("password", MYSQL_CONFIG["password"]) \
        .load() 

       
    if df.count() == 0:
        print("Dataframe is empty")
        return    

    parquet_path = f"s3a://{BUCKET_NAME}/bronze/{table_name}/"
    df.write.format("parquet") \
            .option("path", parquet_path) \
            .mode("append") \
            .saveAsTable(f"dwh.raw_{table_name}")
    print(f"Data saved: {parquet_path}")


def extract_postgres_and_save(table_name):
    last_loaded = get_last_loaded(table_name)
    jdbc_url = POSTGRES_CONFIG["postgres_url"]
    
    
    if last_loaded == None:
        query = f"(SELECT *, NOW() AS raw_extraction_time FROM {table_name})"
    else:
        query = f"""
        (
            SELECT *, NOW() AS raw_extraction_time 
            FROM {table_name}
            WHERE created_at > '{last_loaded}' OR updated_at > '{last_loaded}'
        )
        """   

    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("query", query) \
        .option("user", POSTGRES_CONFIG["user"]) \
        .option("password", POSTGRES_CONFIG["password"]) \
        .load()
    

    if df.count() == 0:
        print("Dataframe is empty")
        return

    parquet_path = f"s3a://{BUCKET_NAME}/bronze/{table_name}/"
    # df.write.mode("append").parquet(parquet_path)
    df.write.format("parquet") \
        .option("path", parquet_path) \
        .mode("append") \
        .saveAsTable(f"dwh.raw_{table_name}")

    print(f"Data saved: {parquet_path}")



def extract_all_and_save():
    tables_mysql = ["categories", "brands", "products"]
    tables_postgres = ["customers", "stores", "staffs", "orders", "order_items"]

    # Extract and save all tables from Mysql OLTP database
    for table_name in tables_mysql:
        extract_mysql_and_save(table_name, False)

    # Extract and save all tables from Postgres OLTP database
    for table_name in tables_postgres:
        extract_postgres_and_save(table_name)

    # Extract stock snapshot
    extract_mysql_and_save("stocks", True)

extract_all_and_save()
spark.stop()