import logging
from pyspark.sql.functions import col, udf, current_timestamp, rand, when
from pyspark.sql.types import FloatType, StringType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mocking the HuggingFace Pipeline for Phase 1/2 Local testing
# In Production, this would be: 
# from transformers import pipeline
# sentiment_pipeline = pipeline("sentiment-analysis", model="cardiffnlp/twitter-roberta-base-sentiment-latest")
def mock_huggingface_inference(text):
    """
    Mock UDF to simulate HuggingFace sentiment inference.
    Returns a score between -1.0 (Negative) and 1.0 (Positive).
    """
    if not text:
        return 0.0
        
    text_lower = text.lower()
    if "historic" in text_lower or "support" in text_lower or "massive step" in text_lower:
        return 0.85
    elif "delay" in text_lower or "disappointed" in text_lower or "hollow" in text_lower:
        return -0.75
    else:
        return 0.10

def create_gold_sentiment(spark):
    logger.info("Starting Gold Layer Sentiment Inference...")
    
    try:
        logger.info("Reading Silver cleaned posts from hive_metastore.silver.posts_cleaned...")
        silver_df = spark.table("hive_metastore.silver.posts_cleaned")
        
        # Register UDF
        sentiment_udf = udf(mock_huggingface_inference, FloatType())
        
        # Apply ML Inference
        logger.info("Applying HuggingFace Sentiment pipeline (Mocked UDF)...")
        gold_df = silver_df.withColumn("sentiment_score", sentiment_udf(col("content")))
        
        # Categorize Sentiment based on score
        gold_df = gold_df.withColumn(
            "sentiment_label",
            when(col("sentiment_score") > 0.3, "Positive")
            .when(col("sentiment_score") < -0.3, "Negative")
            .otherwise("Neutral")
        )
        
        # Add Gold Lineage
        gold_df = gold_df.withColumn("_gold_processed_at", current_timestamp())
        
        # Write out to Gold analytics table
        target_table = "hive_metastore.gold.sentiment_analytics"
        logger.info(f"Writing derived insights to Gold Delta Table: {target_table}")
        spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.gold")
        gold_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
        
        logger.info("Gold layer processing complete. Data ready for dashboard!")
        
    except Exception as e:
        logger.error(f"Gold processing failed: {e}")

if __name__ == "__main__":
    try:
        spark
    except NameError:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("GoldSentiment").getOrCreate()
        
    create_gold_sentiment(spark)
