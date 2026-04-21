import logging
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit, coalesce
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_bronze_to_silver(spark):
    logger.info("Starting Silver Layer Transformation...")
    
    # 1. Process Tweets (Bronze)
    try:
        logger.info("Reading Bronze Tweets...")
        # In a real Databricks environment, you might use: spark.table("hive_metastore.bronze.tweets")
        # Here we use delta paths
        tweets_df = spark.read.format("delta").load("mnt/delta/bronze/tweets")
        
        # Clean: remove nulls in text, normalize schema
        clean_tweets = tweets_df.filter(col("text").isNotNull() & (col("text") != "")) \
            .select(
                col("id").alias("post_id"),
                to_timestamp(col("created_at")).alias("post_timestamp"),
                col("text").alias("content"),
                col("user_id").alias("author"),
                col("source")
            )
            
        logger.info(f"Cleaned {clean_tweets.count()} tweets.")
    except Exception as e:
        logger.warning(f"Could not load bronze tweets. Error: {e}")
        clean_tweets = None


        
    # 2. Assign unified dataframe (The Silver Table)
    if clean_tweets:
        unified_df = clean_tweets
    else:
        logger.error("No bronze data found to process!")
        return
        
    # Deduplicate based on ID (Mistake 4 fix from architecture)
    silver_df = unified_df.dropDuplicates(["post_id"])
    
    # Add Silver Lineage
    silver_df = silver_df.withColumn("_silver_processed_at", current_timestamp())
    
    # Write to Silver layer
    target_path = "mnt/delta/silver/posts_cleaned"
    logger.info(f"Writing {silver_df.count()} unified records to Silver Delta Table: {target_path}")
    silver_df.write.format("delta").mode("overwrite").save(target_path)
    
    logger.info("Silver layer processing complete.")

if __name__ == "__main__":
    # In a Databricks Notebook, 'spark' is implicitly available.
    try:
        spark
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("SilverCleaning").getOrCreate()
    
    clean_bronze_to_silver(spark)
