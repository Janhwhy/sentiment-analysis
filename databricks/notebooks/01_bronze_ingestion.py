import os
import logging
import requests
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Topics to search — tweets matching EITHER query are ingested
SEARCH_QUERIES = [
    '"Women\'s Reservation Bill"',
    '"Delimitation 2026"',
]

# Twitter API v2 recent-search endpoint
TWITTER_SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"

# Max results per query (10–100)
MAX_RESULTS_PER_QUERY = 100

# Target Databricks table
BRONZE_TABLE = "hive_metastore.bronze.tweets"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_bearer_token():
    """
    Retrieve the Twitter Bearer Token.
    In Databricks: pulled from dbutils secrets (recommended for production).
    Locally / in CI: falls back to the TWITTER_BEARER_TOKEN environment variable.
    """
    try:
        # Databricks-native secret store using the twitter_scope we created
        api_key = dbutils.secrets.get(scope="twitter_scope", key="api_key")  # type: ignore[name-defined]
        api_secret = dbutils.secrets.get(scope="twitter_scope", key="api_secret")  # type: ignore[name-defined]
        token = dbutils.secrets.get(scope="twitter_scope", key="bearer_token")  # type: ignore[name-defined]
        
        logger.info("Loaded API Key, API Secret, and Bearer Token from Databricks Secrets.")
        return token
    except Exception:
        token = os.environ.get("TWITTER_BEARER_TOKEN")
        if not token:
            raise EnvironmentError(
                "TWITTER_BEARER_TOKEN not found. "
                "Set it in Databricks Secrets or as an environment variable."
            )
        logger.info("Loaded Bearer Token from environment variable.")
        return token


def fetch_tweets(query: str, bearer_token: str) -> list[dict]:
    """
    Call Twitter API v2 recent-search for a single query string.
    Returns a list of tweet dicts with fields: id, created_at, text, author_id.
    """
    headers = {"Authorization": f"Bearer {bearer_token}"}
    params = {
        "query": f"{query} lang:en -is:retweet",
        "max_results": MAX_RESULTS_PER_QUERY,
        "tweet.fields": "id,created_at,text,author_id",
    }

    logger.info(f"Fetching tweets for query: {query}")
    response = requests.get(TWITTER_SEARCH_URL, headers=headers, params=params, timeout=30)

    if response.status_code == 200:
        data = response.json()
        tweets = data.get("data", [])
        meta = data.get("meta", {})
        logger.info(
            f"  → Retrieved {meta.get('result_count', len(tweets))} tweets "
            f"(newest_id={meta.get('newest_id', 'N/A')})"
        )
        return tweets
    elif response.status_code == 429:
        logger.warning("Rate limit hit (429). Try again after the window resets (15 min).")
        return []
    else:
        logger.error(
            f"Twitter API error {response.status_code} for query '{query}': {response.text}"
        )
        response.raise_for_status()


# ---------------------------------------------------------------------------
# Main ingestion logic
# ---------------------------------------------------------------------------

def main():
    bearer_token = get_bearer_token()

    # --- Fetch from all topics, deduplicate by tweet id ---
    all_tweets: dict[str, dict] = {}
    for query in SEARCH_QUERIES:
        tweets = fetch_tweets(query, bearer_token)
        for t in tweets:
            all_tweets[t["id"]] = t   # dict keyed by id → natural dedup

    if not all_tweets:
        logger.warning("No tweets fetched. Exiting without writing.")
        return

    logger.info(f"Total unique tweets fetched across all topics: {len(all_tweets)}")

    # --- Convert to PySpark DataFrame ---
    rows = [
        Row(
            id=t["id"],
            created_at=t.get("created_at", ""),
            text=t.get("text", ""),
            author_id=t.get("author_id", ""),
        )
        for t in all_tweets.values()
    ]

    tweets_df = spark.createDataFrame(rows)  # type: ignore[name-defined]  — Databricks-provided global

    # Add data lineage columns
    tweets_df = (
        tweets_df
        .withColumn("_loaded_at", current_timestamp())
        .withColumn("_notebook_version", lit("v2.0"))
        .withColumn("_topic", lit(", ".join(SEARCH_QUERIES)))
    )

    # --- Write to Databricks bronze table (append so reruns don't clobber) ---
    logger.info(f"Writing {tweets_df.count()} tweets to {BRONZE_TABLE} ...")

    # Ensure the database exists
    spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.bronze")  # type: ignore[name-defined]

    tweets_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(BRONZE_TABLE)

    logger.info(f"✅ Successfully wrote {tweets_df.count()} tweets to {BRONZE_TABLE}.")
    logger.info("Bronze ingestion complete — data is ready for Silver cleaning.")


if __name__ == "__main__":
    main()
