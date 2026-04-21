import os
import sys
import logging
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit

# Set up logging following production best practices
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_windows_env():
    """Sets up winutils.exe for PySpark on Windows to prevent HADOOP_HOME crashes."""
    if os.name == 'nt':
        logger.info("Windows detected. Setting up mock HADOOP_HOME for local PySpark execution...")
        current_dir = os.getcwd()
        if os.path.basename(current_dir) == "notebooks":
            project_root = os.path.dirname(os.path.dirname(current_dir))
        elif os.path.basename(current_dir) == "databricks":
            project_root = os.path.dirname(current_dir)
        else:
            project_root = current_dir
            
        hadoop_home = os.path.join(project_root, ".hadoop")
        bin_dir = os.path.join(hadoop_home, "bin")
        os.makedirs(bin_dir, exist_ok=True)
        
        winutils_path = os.path.join(bin_dir, "winutils.exe")
        hadoop_dll_path = os.path.join(bin_dir, "hadoop.dll")
        
        try:
            if not os.path.exists(winutils_path):
                logger.info("Downloading winutils.exe...")
                urllib.request.urlretrieve("https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/winutils.exe", winutils_path)
            
            if not os.path.exists(hadoop_dll_path):
                logger.info("Downloading hadoop.dll...")
                urllib.request.urlretrieve("https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.2.2/bin/hadoop.dll", hadoop_dll_path)
                
            os.environ["HADOOP_HOME"] = hadoop_home
            os.environ["PATH"] += os.pathsep + bin_dir
            logger.info("Windows Hadoop environment configured.")
        except Exception as e:
            logger.warning(f"Failed to download winutils: {e}. Spark might still crash.")

def main():
    setup_windows_env()
    
    logger.info("Initializing Spark Session with Delta Lake support...")
    
    # Initialize Spark with Delta configurations
    spark = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
        
    logger.info("Spark session created successfully.")

    # Paths configuration
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
                             
        # Write to Bronze layer
        target_path = os.path.join(bronze_dir, "tweets")
        logger.info(f"Writing Twitter data to: {target_path}")
        
        try:
            tweets_df.write.format("delta").mode("overwrite").save(target_path)
            logger.info(f"Successfully processed {tweets_df.count()} tweets to Delta Bronze layer.")
        except Exception as e:
             logger.error(f"Failed to write to Delta: {e}")
             logger.info("Falling back to parquet...")
             tweets_df.write.format("parquet").mode("overwrite").save(target_path + "_parquet")
    else:
        logger.warning(f"Could not find mock tweets at {tweets_path}")


    logger.info("Bronze ingestion complete! Phase 1 data is in the Lakehouse.")

if __name__ == "__main__":
    main()
