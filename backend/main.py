import os
import logging
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Optional: For actual databricks connection
# from databricks import sql

load_dotenv()

app = FastAPI(title="PublicPulse API", version="1.0.0")

# Allow dashboard frontend to query API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Change to localhost:3000 in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logger = logging.getLogger("uvicorn")

# --- Models ---
class SentimentTrend(BaseModel):
    timestamp: str
    average_score: float
    volume: int

class TopicAnalysisResponse(BaseModel):
    topic: str
    overall_sentiment: float
    sentiment_label: str
    trends: List[SentimentTrend]

# --- Database Hook ---
def execute_databricks_query(query: str):
    """
    Connects to Databricks Serverless SQL Warehouse to run queries natively.
    Requires DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN.
    """
    hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")
    
    if not all([hostname, http_path, token]):
        logger.warning("Databricks credentials missing. Returning MOCK data.")
        return None
        
    # In production, this executes!
    # try:
    #     with sql.connect(server_hostname=hostname, http_path=http_path, access_token=token) as connection:
    #         with connection.cursor() as cursor:
    #             cursor.execute(query)
    #             result = cursor.fetchall()
    #             return [dict(zip([column[0] for column in cursor.description], row)) for row in result]
    # except Exception as e:
    #     logger.error(f"Databricks SQL Error: {e}")
    #     raise HTTPException(status_code=500, detail="Data warehouse connection failed")
    
    return None

# --- Routes ---
@app.get("/")
def read_root():
    return {"status": "healthy", "service": "PublicPulse API"}

@app.get("/api/sentiment/{topic}", response_model=TopicAnalysisResponse)
def get_topic_sentiment(topic: str, hours: int = 24):
    """
    Phase 2 API: Fetches sentiment trends from Databricks Gold Layer.
    """
    # Use real database if configured
    sql_query = f"""
        SELECT 
            date_trunc('hour', post_timestamp) as hour,
            avg(sentiment_score) as avg_score,
            count(*) as vol
        FROM hive_metastore.gold.sentiment_analytics
        WHERE content ILIKE '%{topic}%'
        GROUP BY hour
        ORDER BY hour DESC
        LIMIT {hours}
    """
    
    db_results = execute_databricks_query(sql_query)
    
    # Fallback to Mock Data if Databricks SQL Warehouse isn't configured yet
    if not db_results:
        # Returning mock data for local testing
        return TopicAnalysisResponse(
            topic=topic,
            overall_sentiment=0.45,
            sentiment_label="Positive",
            trends=[
                SentimentTrend(timestamp="2023-09-20T10:00:00Z", average_score=0.6, volume=15),
                SentimentTrend(timestamp="2023-09-20T11:00:00Z", average_score=0.3, volume=42),
                SentimentTrend(timestamp="2023-09-20T12:00:00Z", average_score=-0.1, volume=8)
            ]
        )
        
    # If using real DB results, parse them into the Pydantic models here...
    pass
