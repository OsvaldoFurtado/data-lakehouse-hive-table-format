from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import initcap, trim , lower, col, max, concat_ws, lit, row_number, when, md5
from datetime import datetime



EOW_DATE = datetime(9999, 12, 31)
POSTGRES_SERVICE_CONFIG = {
    "host": "services-db",
    "port": 5432,
    "dbname": "loading_db",
    "user": "postgres",
    "password": "password"
}
BUCKET_NAME = "lakehouse"


# Cria a SparkSession
spark = SparkSession.builder \
    .appName("Silver Job") \
    .enableHiveSupport() \
    .getOrCreate()


def column_renamer(df, suffix, append):
    """
    input:
        df: dataframe
        suffix: suffix to be appended to column name
        append: boolean value 
                if true append suffix else remove suffix
    
    output:
        df: df with renamed column
    """
    if append:
        new_column_names = list(map(lambda x: x+suffix, df.columns))
    else:
        new_column_names = list(map(lambda x: x.replace(suffix,""), df.columns))
    return df.toDF(*new_column_names)


def get_hash(df, keys_list):
    """
    input:
        df: dataframe
        key_list: list of columns to be hashed    
    output:
        df: df with hashed column
    """
    columns = [col(column) for column in keys_list]
    if columns:
        return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
    else:
        return df.withColumn("hash_md5", md5(lit(1)))



def get_last_loaded(table_name):
    silver_path = f"s3a://{BUCKET_NAME}/silver/{table_name}/"
    try:
        df = spark.read.parquet(silver_path)
        last_extraction_time = df.agg(max("extraction_bronze")).collect()[0][0]
        return last_extraction_time
    except:
        return None  


def extract_bronze(table_name, aux_table):
    bronze_path = f"s3a://{BUCKET_NAME}/bronze/{table_name}/"
    last_loaded = get_last_loaded(table_name)

    if last_loaded == None or aux_table:
        df = spark.read.parquet(bronze_path) \
                    .dropDuplicates() \
                    .withColumn("extraction_bronze",lit(datetime.now()))
    else:
        df = spark.read.parquet(bronze_path) \
                    .dropDuplicates() \
                    .filter((col("raw_extraction_time") > last_loaded)) \
                    .withColumn("extraction_bronze",lit(datetime.now()))
    return df



# SCD Type 2 
def scd_type2(table_name, df_type2_cols, business_key, df_current, df_window_spec):

    df_history = spark.read.parquet(f"s3a://lakehouse/silver/{table_name}/")
    
    max_sk = df_history.agg({f"sk_{business_key}": "max"}).collect()[0][0]

    df_history_open = df_history.where(col("current_flag"))
    df_history_closed = df_history.where(col("current_flag")==lit(False))
    df_history_closed.cache()

    df_history_open_hash = column_renamer(get_hash(df_history_open, df_type2_cols), suffix="_history", append=True)
    df_current_hash = column_renamer(get_hash(df_current, df_type2_cols), suffix="_current", append=True)

    df_merged = df_history_open_hash\
                    .join(df_current_hash, col(f"{business_key}_current") ==  col(f"{business_key}_history"), how="full_outer")\
                    .withColumn("Action", when(col("hash_md5_current") == col("hash_md5_history")  , 'NOCHANGE')\
                    .when(col(f"{business_key}_current").isNull(), 'DELETE')\
                    .when(col(f"{business_key}_history").isNull(), 'INSERT')\
                    .otherwise('UPDATE'))

    df_nochange = column_renamer(df_merged.filter(col("action") == 'NOCHANGE'), suffix="_history", append=False)\
                    .select(df_history_open.columns)

    df_insert = column_renamer(df_merged.filter(col("action") == 'INSERT'), suffix="_current", append=False)\
                    .select(df_current.columns)\
                    .withColumnRenamed("created_at", "start_date") \
                    .withColumn("end_date", lit(EOW_DATE))\
                    .withColumn("row_number",row_number().over(df_window_spec))\
                    .withColumn(f"sk_{business_key}",col("row_number")+ max_sk)\
                    .withColumn("current_flag", lit(True))\
                    .drop("row_number") \
                    .drop("updated_at")

    max_sk = df_insert.agg({f"sk_{business_key}": "max"}).collect()[0][0]

    df_deleted = column_renamer(df_merged.filter(col("action") == 'DELETE'), suffix="_history", append=False)\
                    .select(df_history_open.columns)\
                    .withColumn("end_date", lit(datetime.now()))\
                    .withColumn("current_flag", lit(False))

    df_updated = column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_history", append=False)\
                    .withColumn("end_date", col("updated_at_current"))\
                    .select(df_history_open.columns)\
                    .withColumn("current_flag", lit(False))\
                    .unionByName(
                    column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_current", append=False)\
                        .select(df_current.columns)\
                        .withColumnRenamed("created_at", "start_date")\
                        .withColumn("end_date",lit(EOW_DATE))\
                        .withColumn("row_number",row_number().over(df_window_spec))\
                        .withColumn(f"sk_{business_key}",col("row_number")+ max_sk)\
                        .withColumn("current_flag", lit(True))\
                        .drop("row_number")
                        .drop("updated_at")
                        )

    # Final
    final_df = df_history_closed\
                .unionByName(df_nochange)\
                .unionByName(df_insert)\
                .unionByName(df_deleted)\
                .unionByName(df_updated)
    
    final_df.write.format("parquet") \
            .mode("overwrite") \
            .option("path", f"s3a://lakehouse/silver/{table_name}/") \
            .saveAsTable(f"dwh.silver_{table_name}")




def save_scd2_silver_data(table_name,df,key,scd2_cols):
    window_spec  = Window.orderBy(key)
    last_loaded = get_last_loaded(table_name)
    if last_loaded != None:
        scd_type2(table_name, scd2_cols, key, df, window_spec)    
    else:
        df \
                        .withColumn(f"sk_{key}",row_number().over(window_spec)) \
                        .withColumnRenamed("created_at", "start_date") \
                        .withColumn("end_date", lit(EOW_DATE)) \
                        .withColumn("current_flag", lit(True)) \
                        .drop("updated_at") \
                        .write.format("parquet") \
                        .mode("overwrite") \
                        .option("path", f"s3a://lakehouse/silver/{table_name}/") \
                        .saveAsTable(f"dwh.silver_{table_name}")



# Load Bronze data
bronze_customers_df = extract_bronze("customers", False)
bronze_products_df = extract_bronze("products", False)
bronze_brands_df = extract_bronze("brands", True)
bronze_categories_df = extract_bronze("categories", True)
bronze_staffs_aux_df = extract_bronze("staffs", True)
bronze_staffs_df = extract_bronze("staffs", False)
bronze_orders_df = extract_bronze("orders", False)
bronze_order_items_df = extract_bronze("order_items", False)
bronze_stores_df = extract_bronze("stores", False)
bronze_stores_aux_df = extract_bronze("stores", True)
bronze_stocks_df = extract_bronze("stocks", False)


#scd_type_2 tables
customers_scd2_cols = ["email","street","city","state","zip_code"]
products_scd2_cols = ["list_price"]
staffs_scd2_cols = ["store_name","manager_first_name","manager_last_name"]

if bronze_customers_df.count() > 0:
    silver_customers_df =  bronze_customers_df.fillna({'email': 'unknown', 'phone': 'unknown'}) \
                                        .withColumn("first_name", initcap(trim(col("first_name")))) \
                                        .withColumn("last_name", initcap(trim(col("last_name")))) \
                                        .withColumn("email", lower(col("email")))
    save_scd2_silver_data("customers",silver_customers_df,"customer_id",customers_scd2_cols)

if bronze_products_df.count() > 0:
# Join brand and Categories data
    silver_products_df = bronze_products_df.alias("product") \
                                        .join(bronze_brands_df.alias("brand"), "brand_id", "left") \
                                        .join(bronze_categories_df.alias("category"), "category_id", "left") \
                                        .select(
                                            col("product.product_id"),
                                            col("product.product_name"),
                                            col("product.model_year"),
                                            col("product.list_price"),
                                            col("brand.brand_name"),
                                            col("category.category_name"),
                                            col("product.created_at"),
                                            col("product.updated_at")                                      
                                        )
    save_scd2_silver_data("products",silver_products_df,"product_id",products_scd2_cols)


if bronze_staffs_df.count() > 0:
# staffs
# Replace NULL with empty strings
# Join Manager Data
# Join Staff store data
    silver_staffs_df = bronze_staffs_df.fillna({'manager_id': 'without manager'})
    silver_staffs_df = silver_staffs_df.alias("staff") \
                                        .join(bronze_staffs_aux_df.alias("manager"), col("staff.manager_id") == col("manager.staff_id"), "left") \
                                        .join(bronze_stores_aux_df.alias("store"), col("staff.store_id") == col("store.store_id"), "left") \
                                        .select(
                                            col("staff.staff_id"), 
                                            col("staff.first_name"), 
                                            col("staff.last_name"), 
                                            col("staff.email"), 
                                            col("staff.phone"), 
                                            col("staff.active"), 
                                            col("store.store_name"), 
                                            col("manager.first_name").alias("manager_first_name"), 
                                            col("manager.last_name").alias("manager_last_name"), 
                                            col("staff.created_at"),
                                            col("staff.updated_at")
                                        )
    save_scd2_silver_data("staffs",silver_staffs_df,"staff_id",products_scd2_cols)


# orders
# Replace NULL with empty strings
if bronze_orders_df.count() > 0:
    silver_orders_df = bronze_orders_df.fillna({'shipped_date': 'not shipped'})
    silver_orders_df \
                            .drop("created_at") \
                            .drop("updated_at") \
                            .write.format("parquet") \
                            .mode("append") \
                            .option("path", f"s3a://lakehouse/silver/orders/") \
                            .saveAsTable(f"dwh.silver_orders")

# order_items
# Replace NULL discounts with value 0
if bronze_order_items_df.count() > 0:
    silver_order_items_df = bronze_order_items_df.fillna({'discount': 0})
    silver_order_items_df \
                            .drop("created_at") \
                            .drop("updated_at") \
                            .withColumn("total", col("quantity") * col("list_price") * (1 - col("discount"))) \
                            .write.format("parquet") \
                            .mode("append") \
                            .option("path", f"s3a://lakehouse/silver/order_items/") \
                            .saveAsTable(f"dwh.silver_order_items")

if bronze_stores_df.count() > 0:
    bronze_stores_df \
                            .drop("created_at") \
                            .drop("updated_at") \
                            .write.format("parquet") \
                            .mode("append") \
                            .option("path", f"s3a://lakehouse/silver/stores/") \
                            .saveAsTable(f"dwh.silver_stores")



if bronze_stocks_df.count() > 0:
    bronze_stocks_df \
                            .drop("created_at") \
                            .drop("updated_at") \
                            .withColumnRenamed("raw_extraction_time", "snapshot_time") \
                            .write.format("parquet") \
                            .mode("append") \
                            .option("path", f"s3a://lakehouse/silver/stocks/") \
                            .saveAsTable(f"dwh.silver_stocks")

spark.stop()






