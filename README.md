# 🚀 Sentiment Analysis

Sentiment Analysis is a real-time public sentiment intelligence platform that analyzes online discussions (Twitter/X, News) to track how public opinion evolves over time.

It combines **data engineering, NLP, and full-stack development** into a production-style project using Databricks and Antigravity.

---

## 📌 Overview

PublicPulse allows users to:

- Analyze public sentiment strictly on the **Women's Quota Bill and Delimitation, 2026**
- Analyze sentiment (positive / neutral / negative)
- Track sentiment trends over time

- Discover trending keywords
- View viral posts
- Generate AI-based insights

---

## 🏗️ System Architecture

```
Data Sources (Twitter / News)
        ↓
Antigravity (Workflow Orchestration)
        ↓
Databricks (ETL + NLP + Delta Lake)
        ↓
FastAPI Backend (APIs)
        ↓
Next.js Frontend (Dashboard)
```

---

## ⚙️ Tech Stack

### Frontend
- Next.js
- TypeScript
- Tailwind CSS
- Recharts

### Backend
- FastAPI
- Python

### Data & ML
- Databricks
- PySpark
- Delta Lake
- HuggingFace Transformers
- Pandas / Scikit-learn

### Orchestration
- Antigravity

### Infra
- Docker
- Redis (caching)
- Pytest (testing)

---

## 📁 Project Structure

```
publicpulse/
├── backend/           # FastAPI APIs
├── frontend/          # Next.js dashboard
├── databricks/        # ETL + NLP pipelines
├── orchestration/     # Antigravity workflows
├── tests/             # Unit & integration tests
├── data/              # Sample datasets
├── docs/              # Docs (deployment, interview)
```

---

## 🚀 Getting Started

### 1. Clone Repository

```
git clone https://github.com/your-username/publicpulse.git
cd publicpulse
```

---

### 2. Backend Setup

```
pip install -r requirements.txt
uvicorn backend.main:app --reload
```

Backend runs on:
```
http://localhost:8000
```

---

### 3. Frontend Setup

```
cd frontend
npm install
npm run dev
```

Frontend runs on:
```
http://localhost:3000
```

---

### 4. Databricks

- Import notebooks from `databricks/notebooks/`
- Run:
  - Bronze ingestion
  - Silver cleaning
  - Gold sentiment + analytics

---

### 5. Antigravity

- Deploy workflows from `orchestration/antigravity/`
- Schedule:
  - Data ingestion
  - Sentiment jobs
  - Alerts

---

## 🧪 Running Tests

```
pytest tests/
```

---

## 🎯 MVP Scope


- Sentiment classification  
- Basic dashboard (trend chart)  

---

## 🚀 Future Improvements

- Real-time streaming pipelines  
- Forecasting (Prophet / LSTM)  
- Alert system  
- Multi-topic comparison  
- SaaS deployment  

---

## 💼 Resume Highlights

- Built end-to-end sentiment analysis platform using Databricks + Antigravity  
- Designed scalable data pipelines using medallion architecture  
- Implemented NLP models for sentiment and keyword extraction  
- Developed full-stack dashboard with FastAPI and Next.js  

---

## 🤝 Contributors

- Janhavi Bhattad
- Rudra Solanki  

---

## 📜 License

MIT License