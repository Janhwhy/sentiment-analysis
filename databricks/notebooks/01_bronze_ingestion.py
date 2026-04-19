import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit

# Set up logging following production best practices
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Initializing Spark Session with Delta Lake support...")
    
    # Initialize Spark with Delta configurations
    # Note: On a real Databricks cluster, you don't need to configure packages directly.
    # We do this for local development with delta-spark.
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    logger.info("Spark session created successfully.")

    # Paths configuration
    # This ensures the script can be run from project root or databricks/notebooks folder
    current_dir = os.getcwd()
    if os.path.basename(current_dir) == "notebooks":
        project_root = os.path.dirname(os.path.dirname(current_dir))
    elif os.path.basename(current_dir) == "databricks":
        project_root = os.path.dirname(current_dir)
    else:
        project_root = current_dir
        
    data_dir = os.path.join(project_root, "data")
    bronze_dir = os.path.join(project_root, "mnt", "delta", "bronze")
    
    logger.info(f"Using data directory: {data_dir}")
    logger.info(f"Target bronze directory: {bronze_dir}")

    # ===== Process Tweets =====
    tweets_path = os.path.join(data_dir, "sample_tweets.json")
    if os.path.exists(tweets_path):
        logger.info("Processing Twitter data...")
        
        # Read JSON with multiline enabled
        tweets_df = spark.read.option("multiline", "true").json(tweets_path)
        
        # Add tracking metadata (Data Lineage Best Practice)
        tweets_df = tweets_df.withColumn("_source_file", input_file_name()) \
                             .withColumn("_loaded_at", current_timestamp()) \
                             .withColumn("_notebook_version", lit("v1.0"))
                             
        # Write to Bronze layer in Delta format
        target_path = os.path.join(bronze_dir, "tweets")
        logger.info(f"Writing Twitter data to: {target_path}")
        
        # Idempotent write (overwrite or append depending on strategy)
        # Using overwrite for mock setup to prevent accidental duplicates during testing
        try:
            tweets_df.write.format("delta").mode("overwrite").save(target_path)
            logger.info(f"Successfully processed {tweets_df.count()} tweets to Delta Bronze layer.")
        except Exception as e:
             logger.error(f"Failed to write to Delta. (If local winutils is missing, try parquet). Error: {e}")
             logger.info("Falling back to parquet just in case local Delta setup is missing dependencies.")
             tweets_df.write.format("parquet").mode("overwrite").save(target_path + "_parquet")
    else:
        logger.warning(f"Could not find mock tweets at {tweets_path}")

    # ===== Process Reddit =====
    reddit_path = os.path.join(data_dir, "sample_reddit.json")
    if os.path.exists(reddit_path):
        logger.info("Processing Reddit data...")
        reddit_df = spark.read.option("multiline", "true").json(reddit_path)
        
        reddit_df = reddit_df.withColumn("_source_file", input_file_name()) \
                             .withColumn("_loaded_at", current_timestamp()) \
                             .withColumn("_notebook_version", lit("v1.0"))
                             
        target_path = os.path.join(bronze_dir, "reddit")
        logger.info(f"Writing Reddit data to: {target_path}")
        
        try:
            reddit_df.write.format("delta").mode("overwrite").save(target_path)
            logger.info(f"Successfully processed {reddit_df.count()} reddit posts to Delta Bronze layer.")
        except Exception as e:
            logger.error(f"Failed to write Delta, falling back to parquet. Error: {e}")
            reddit_df.write.format("parquet").mode("overwrite").save(target_path + "_parquet")
    else:
        logger.warning(f"Could not find mock reddit posts at {reddit_path}")
        
    logger.info("Bronze ingestion complete! Phase 1 data is in the Lakehouse.")

if __name__ == "__main__":
    main()
