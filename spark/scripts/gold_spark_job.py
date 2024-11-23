from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, quarter, col, lit
from datetime import datetime

POSTGRES_SERVICE_CONFIG = {
    "host": "services-db",
    "port": 5432,
    "dbname": "loading_db",
    "user": "postgres",
    "password": "password"
}
BUCKET_NAME = "lakehouse"

# Create SparkSession
spark = SparkSession.builder \
    .appName("Create Gold Data") \
    .enableHiveSupport() \
    .getOrCreate()



def get_last_loaded(table_name):
    gold_path = f"s3a://{BUCKET_NAME}/gold/{table_name}/"
    try:
        df = spark.read.parquet(gold_path)
        last_extraction_time = df.agg(max("extraction_silver")).collect()[0][0]
        return last_extraction_time
    except:
        return None  


def extract_silver_data(gold_name, silver_name, dim, aux):
    silver_path = f"s3a://lakehouse/silver/{silver_name}"
    gold_path = f"s3a://lakehouse/gold/{gold_name}"

    if dim:
        df = spark.read.parquet(silver_path).dropDuplicates()
        if not aux:
            df.write.format("parquet") \
                    .mode("overwrite") \
                    .option("path", gold_path) \
                    .saveAsTable(f"dwh.{gold_name}") 
                
        return df
    
    else:
        last_loaded = get_last_loaded(gold_name)
        if last_loaded == None:
            df = spark.read.parquet(silver_path) \
                            .dropDuplicates() \
                            .withColumn("extraction_silver",lit(datetime.now()))
        else:
            df = spark.read.parquet(silver_path) \
                            .dropDuplicates() \
                            .filter((col("raw_extraction_time") > last_loaded)) \
                            .withColumn("extraction_silver",lit(datetime.now()))
        return df





# Read Silver Data
silver_orders_df = extract_silver_data("orders", "orders", True, True)
silver_products_df = extract_silver_data("dim_products", "products", True, False)
silver_stores_df = extract_silver_data("dim_stores", "stores", True, False)
silver_customers_df = extract_silver_data("dim_customers", "customers", True, False)
silver_staffs_df = extract_silver_data("dim_staffs", "staffs", True, False)
silver_order_items_df = extract_silver_data("fact_sales", "order_items", False, False)
silver_stocks_df = extract_silver_data("fact_stocks", "stocks", False, False)


# Create Sales Fact
if silver_order_items_df.count() > 0:
    fact_sales_df = silver_order_items_df.alias("ord_items") \
                                        .join(silver_orders_df.alias("orders"), "order_id", "left") \
                                        .join(silver_customers_df.alias("c"), (col("orders.customer_id") == col("c.customer_id")) &
                                            (col("orders.order_date").between(col("c.start_date"),col("c.end_date"))),
                                            "left") \
                                        .join(silver_products_df.alias("p"), (col("ord_items.product_id") == col("p.product_id")) &
                                            (col("orders.order_date").between(col("p.start_date"),col("p.end_date"))),
                                            "left") \
                                        .join(silver_staffs_df.alias("s"), (col("orders.staff_id") == col("s.staff_id")) &
                                            (col("orders.order_date").between(col("s.start_date"),col("s.end_date"))),
                                            "left") \
                                        .select("order_id", "sk_customer_id", "store_id", 
                                                "sk_staff_id", "order_date", "sk_product_id", 
                                                "quantity", "ord_items.list_price", "discount")

    fact_sales_df = fact_sales_df.withColumn("total", col("quantity") * col("list_price") * (1 - col("discount")))
    
    # Extracting year, quarter, month and day from order_date and stocks
    fact_sales_df = fact_sales_df.withColumn("year", year(fact_sales_df["order_date"])) \
                                .withColumn("quarter", quarter(fact_sales_df["order_date"])) \
                                .withColumn("month", month(fact_sales_df["order_date"])) \
                                .withColumn("day", dayofmonth(fact_sales_df["order_date"]))
    
    # Write Fact Sales partictioned by year, quarter, month and day
    fact_sales_df.write.format("parquet") \
                .mode("append") \
                .option("path", "s3a://lakehouse/gold/fact_sales/") \
                .partitionBy("year", "quarter", "month") \
                .saveAsTable(f"dwh.fact_sales") 
                



# Create Stock Fact
if silver_stocks_df.count() > 0:
    fact_stocks_df = silver_stocks_df.alias("stock") \
                                        .join(silver_products_df.alias("p"), (col("stock.product_id") == col("p.product_id")) &
                                            (col("stock.snapshot_time").between(col("p.start_date"), col("p.end_date"))), 
                                            "left") \
                                        .join(silver_stores_df, "store_id", "left") \
                                        .select("store_id", "sk_product_id", "store_name", "quantity", "list_price", "snapshot_time")

    fact_stocks_df = fact_stocks_df.withColumn("value_stocked", col("quantity") * col("list_price"))

    fact_stocks_df = fact_stocks_df.withColumn("year", year(fact_stocks_df["snapshot_time"])) \
                                        .withColumn("month", month(fact_stocks_df["snapshot_time"])) \
                                        .withColumn("day", dayofmonth(fact_stocks_df["snapshot_time"]))

    # Write Fact Sales partictioned by store
    fact_stocks_df.write.format("parquet") \
                .mode("append") \
                .option("path", "s3a://lakehouse/gold/fact_stocks/") \
                .partitionBy("store_name", "year", "month") \
                .saveAsTable("fact_stocks") \
        





spark.stop()