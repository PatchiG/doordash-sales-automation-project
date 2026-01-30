# DoorDash GTM Sales Intelligence System

An end-to-end automated MVP lead generation and sales intelligence platform built for DoorDash's Go-To-Market team. The system discovers potential merchant partners via the Google Places API, scores and prioritizes leads using engineered features, and provides an AI-powered natural language search interface for the sales team. Currently this is the first version of the whole platform, and further changes and updates will be added based on the future scopr for the project.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Pipeline Components](#pipeline-components)
  - [1. Data Collection](#1-data-collection)
  - [2. Feature Engineering & Lead Scoring](#2-feature-engineering--lead-scoring)
  - [3. Lead Export (CSV)](#3-lead-export-csv)
  - [4. Google Sheets Export](#4-google-sheets-export)
  - [5. RAG Vector Store](#5-rag-vector-store)
  - [6. Query Engine](#6-query-engine)
  - [7. Streamlit App](#7-streamlit-app)
- [Airflow Orchestration](#airflow-orchestration)
- [Usage](#usage)
- [Data Pipeline Flow](#data-pipeline-flow)

---

## Architecture Overview

```
Google Places API
       |
       v
 Data Collection ──> Raw CSV (data/raw/)
       |
       v
Feature Engineering ──> Scored Leads CSV (data/processed/)
       |
       ├──> CSV Export (data/output/)         ──> Sales Team
       ├──> Google Sheets Export              ──> Team Collaboration
       └──> Vector Store (chroma_db/)         ──> RAG Query Engine ──> Streamlit UI
```

The pipeline runs weekly via Apache Airflow, collecting merchant data across five major US cities, scoring each lead on likelihood of conversion, and making the results available through both file exports and an interactive AI-powered dashboard.

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Data Source | Google Places API | Merchant discovery and enrichment |
| Data Processing | Python, Pandas, NumPy | ETL and transformation |
| Orchestration | Apache Airflow | Weekly automated pipeline |
| AI / Embeddings | OpenAI (GPT, text-embedding-3-small) | LLM responses and semantic search |
| Vector Database | ChromaDB | Persistent vector store for lead embeddings |
| RAG Framework | LangChain | Retrieval-augmented generation chain |
| Frontend | Streamlit | Interactive sales intelligence dashboard |
| Collaboration | Google Sheets API (gspread) | Export leads to shared spreadsheets |
| Code Quality | Pytest, Black, Flake8 | Testing and formatting |

---

## Setup & Installation

### Prerequisites

- Python 3.12+
- Google Cloud account (Places API enabled)
- OpenAI API key
- Google service account credentials (for Sheets export)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd doordash-automation-project

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the project root:

```env
# Required
GOOGLE_PLACES_API_KEY=<your-google-places-api-key>
OPENAI_API_KEY=<your-openai-api-key>

# Google Places settings
GOOGLE_PLACES_RADIUS=50000

# Google Sheets (optional, for Sheets export)
GOOGLE_SHEETS_CREDENTIALS_PATH=./credentials.json

# Directories
PROJECT_ROOT=.
DATA_DIR=./data
OUTPUT_DIR=./data/output

# Airflow
AIRFLOW_HOME=./airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__DAGS_FOLDER=./airflow/dags
AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Optional
SLACK_WEBHOOK_URL=<your-slack-webhook-url>
```

---

## Configuration

All pipeline settings are centralized in `src/config.py`:

**Target Cities:** San Francisco, New York, Chicago, Los Angeles, Seattle

**Search Queries:** Pizza restaurants, Chinese restaurants, Mexican restaurants, Grocery stores, Supermarkets, Convenience stores

**Lead Scoring Weights (out of 100):**

| Factor | Points | Description |
|---|---|---|
| Competitor platform presence | 25 | On Uber Eats, Grubhub, etc. |
| Review volume | 5 - 20 | Tiered: 0-50, 50-200, 200-500, 500+ |
| High-demand category | 20 | Pizza, Chinese, Mexican, etc. |
| Urban location | 15 | Located in a target city |
| High rating | 10 | Rating >= 4.0 stars |
| Price accessibility | 10 | Price level 0-2 |

**Vertical Rules:**

| Vertical | Min Score | Target Count | SLA (days) |
|---|---|---|---|
| Restaurants | 50 | 200 | 7 |
| Grocery | 60 | 100 | 14 |
| Retail | 55 | 150 | 10 |

---

## Pipeline Components

### 1. Data Collection

**File:** `src/data_collection.py`

Fetches merchant data from the Google Places API across all configured cities and search queries.

- Geocodes each city to coordinates
- Searches for businesses within a 50km radius per query
- Retrieves detailed information for each result (phone, rating, review count, hours, price level)
- Deduplicates by `place_id`
- Rate-limited to respect API quotas (0.05s between detail requests, 1s between searches)

**Output:** `data/raw/businesses_<timestamp>.csv` (~5,000-10,000 records, 16 columns)

```bash
python src/data_collection.py
```

Data Snapshot:

<img width="1676" height="718" alt="Screenshot 2026-01-28 at 9 17 56 PM" src="https://github.com/user-attachments/assets/19899f56-f85e-4a14-831f-1480963c5b8d" />


---

### 2. Feature Engineering & Lead Scoring

**File:** `src/feature_engineering.py`

Transforms raw business data into scored, prioritized leads.

**Engineered Features:**
- `high_demand_category` - Whether the business is in a high-demand food category
- `review_volume_bucket` - Bucketed review count (low / medium / high / very_high)
- `price_accessible` - Whether the price level is accessible (tiers 0-2)
- `high_rating` - Rating >= 4.0 stars
- `urban_location` - Located in one of the five target cities
- `competitor_platform` - Simulated competitor presence (Uber Eats 60%, Grubhub 50%)
- `vertical` - Business classification (restaurants, grocery, retail)

**Scoring:** Each lead receives a weighted score from 0-100 and a priority classification (Low / Medium / High / Critical) along with a contact-by date based on the vertical's SLA.

**Output:** `data/processed/scored_leads_<timestamp>.csv` (20+ columns)

```bash
python src/feature_engineering.py
```

---

### 3. Lead Export (CSV)

**File:** `src/export_leads.py`

Exports scored leads into separate CSVs by business vertical, applying vertical-specific filters and thresholds.

**Outputs in `data/output/`:**
- `restaurant_leads_week<N>_<year>_<timestamp>.csv`
- `grocery_leads_week<N>_<year>_<timestamp>.csv`
- `retail_leads_week<N>_<year>_<timestamp>.csv`
- `all_leads_combined_week<N>_<year>_<timestamp>.csv`
- `sales_summary_week<N>_<year>_<timestamp>.txt`

```bash
python src/export_leads.py
```

---

### 4. Google Sheets Export

**File:** `src/export_google_sheets.py`

Exports leads to Google Sheets for team collaboration. Requires a Google service account with credentials stored in `credentials.json`.

- Creates one spreadsheet per vertical with formatted headers
- Creates a summary spreadsheet with links to each vertical sheet
- Shares spreadsheets with specified email addresses

```bash
python src/export_google_sheets.py
```

---

### 5. RAG Vector Store

**File:** `rag_system/create_vectorstore.py`

Converts scored leads into vector embeddings for semantic search.

- Loads the latest scored leads CSV
- Converts each lead into a LangChain `Document` with structured text content and metadata
- Embeds documents using OpenAI's `text-embedding-3-small` model
- Persists the vector store to `chroma_db/` using ChromaDB

```bash
python rag_system/create_vectorstore.py
```

---

### 6. Query Engine

**File:** `rag_system/query_engine.py`

Provides a natural language query interface over the leads database using retrieval-augmented generation.

- Loads the persisted Chroma vector store
- Builds a LangChain `RetrievalQA` chain with a custom sales intelligence prompt
- Retrieves the top 5 most relevant leads for each query
- Returns a structured answer with source lead metadata (name, category, city, score, rating, review count)

**Example queries:**
- "Show me pizza restaurants in San Francisco"
- "Find high-priority leads"
- "Which leads are on Uber Eats?"
- "Show me grocery stores with high ratings"

---

### 7. Streamlit App

**File:** `streamlit_app/app.py`

Interactive web dashboard for the sales team.

**Features:**
- Chat-based interface for natural language lead queries
- Expandable source panels showing detailed lead information per result
- Sidebar with pre-built example queries
- Conversation history within a session
- System status display (embedding model, LLM model)

```bash
streamlit run streamlit_app/app.py --server.headless true
```

Access at `http://localhost:8501`.

Access Examples 1:

<img width="1676" height="825" alt="Screenshot 2026-01-28 at 9 18 35 PM" src="https://github.com/user-attachments/assets/18531c22-7fc6-4a64-a1f4-0fd6c7beb82c" />


Access Examples 2:

<img width="1676" height="825" alt="Screenshot 2026-01-28 at 9 20 10 PM" src="https://github.com/user-attachments/assets/d7837446-2b6e-429b-99ef-766fd91846b6" />


Access Examples 3:

<img width="1676" height="825" alt="Screenshot 2026-01-28 at 9 21 01 PM" src="https://github.com/user-attachments/assets/00d68148-829d-474f-8041-109f0fab5135" />

Access Examples 4:

<img width="1676" height="825" alt="Screenshot 2026-01-28 at 9 21 50 PM" src="https://github.com/user-attachments/assets/91b0d54b-131d-4ccb-b9c7-64db5ace34a0" />


---

## Airflow Orchestration

**File:** `airflow/dags/leads_generation.py`

The full pipeline is automated as an Airflow DAG named `weekly_leads_generation`.

**Schedule:** Every Monday at midnight (`0 0 * * 1`)

**Tasks (sequential):**

| # | Task ID | Description |
|---|---|---|
| 1 | `collect_data` | Fetch businesses from Google Places API |
| 2 | `engineer_features` | Clean data, create features, score leads |
| 3 | `export_csv` | Export filtered leads by vertical to CSV |
| 4 | `update_vector_store` | Rebuild vector embeddings in ChromaDB |
| 5 | `generate_notification` | Compile run metrics and generate summary |

**Configuration:**
- 2 retries per task with a 5-minute delay
- Metrics passed between tasks via Airflow XCom
- Email notification on failure (Need to be configured with email)

**Setup:**

```bash
# Initialize Airflow database
airflow db init

# Start the scheduler (background)
airflow scheduler -D

# Optionally start the web UI
airflow webserver -D

# Trigger a manual run
airflow dags trigger weekly_leads_generation
```

<img width="1676" height="875" alt="Screenshot 2026-01-28 at 9 22 15 PM" src="https://github.com/user-attachments/assets/cb88192a-87b1-4ae2-b9e7-70f3166e8b0e" />


---

## Usage

### Run the Full Pipeline Manually

```bash
# 1. Collect merchant data
python src/data_collection.py

# 2. Engineer features and score leads
python src/feature_engineering.py

# 3. Export leads to CSV
python src/export_leads.py

# 4. (Optional) Export to Google Sheets
python src/export_google_sheets.py

# 5. Build the RAG vector store
python rag_system/create_vectorstore.py

# 6. Launch the Streamlit dashboard
streamlit run streamlit_app/app.py --server.headless true
```

### Run with Airflow (Automated Weekly)

```bash
airflow db init
airflow scheduler -D
# Pipeline runs automatically every Monday at midnight
```

<img width="1676" height="875" alt="Screenshot 2026-01-28 at 9 22 34 PM" src="https://github.com/user-attachments/assets/59b66cec-cdf1-4b21-8b38-732c988d042a" />

---

## Current Data Pipeline Flow

```
                    ┌──────────────────────────┐
                    │   Google Places API      │
                    │   5 cities x 6 queries   │
                    └────────────┬─────────────┘
                                 │
                                 v
                    ┌──────────────────────────┐
                    │   data/raw/              │
                    │   businesses_*.csv       │
                    │   ~5,000-10,000 records  │
                    └────────────┬─────────────┘
                                 │
                                 v
                    ┌──────────────────────────┐
                    │   Feature Engineering    │
                    │   + Lead Scoring (0-100) │
                    │   + Priority Assignment  │
                    └────────────┬─────────────┘
                                 │
                                 v
                    ┌──────────────────────────┐
                    │   data/processed/        │
                    │   scored_leads_*.csv     │
                    └──────┬─────┬─────┬───────┘
                           │     │     │
              ┌────────────┘     │     └────────────┐
              v                  v                  v
   ┌─────────────────┐ ┌────────────────┐ ┌─────────────────┐
   │  CSV Export     │ │ Vector Store   │ │ Google Sheets   │
   │  by Vertical    │ │ (ChromaDB)     │ │ Export          │
   │  + Summary      │ │                │ │ (Optional)      │
   └────────┬────────┘ └───────┬────────┘ └─────────────────┘
            │                  │
            v                  v
   ┌─────────────────┐ ┌────────────────┐
   │  data/output/   │ │ RAG Query      │
   │  restaurant_*   │ │ Engine         │
   │  grocery_*      │ │ (LangChain)    │
   │  retail_*       │ └───────┬────────┘
   │  combined_*     │         │
   │  summary_*      │         v
   └─────────────────┘ ┌────────────────┐
                       │ Streamlit App  │
                       │ localhost:8501 │
                       └────────────────┘
```

## Future Plans for Project
1. ### Additional APIs/ Competitor Scrapping Scripts:
   - Yelp Fusion API: Integrate Yelp API in the data collection stage to extract additional local businesses and key information. Yelp generally has better reviews about a business allowing us to better gauge the customers sentiment.
   - Salesforce CRM Integration: Will be integrated in the future (currently do not hold any account).
   - Scrape data from Uber Eats, Instacart, Grubhub: Currently proxy data has been used to justify whether a store is present in uber eats or not. In the future scope I intend to legally scrape or use paid APIs to collect competitors merchants in a particular localty.
  
2. ### Upgrade Current Lead Scoring System:
   - The curren scoring system works on few factors deemed important from my research. But I intend to add additional fields that help in scoring the importance of a lead.
     
3. ### Export to Google Sheets:
   - Currently the script to push data to google sheeets and API have been initiated. But due to exceeding cost issues, I have not added them to the airflow DAGS.
   - Hence they will be further added into the project either by creating an app script that downloads data which will to sent through automated emails and pasted to the google sheets.
     - These sheets will then be shared with the sales executives as an additional copy.

4. ### Streamlit App:
   - Currently the RAG model works on chromadb. Which can be a potential factor for slower loading times and slower response of the streamlit application.
   - The UI for streamlit can be further enhanced to make it user firendly which can be done based on stakeholder inputs and efforts will be made to easily integrate into their current workflow.
   - The categories of the store/restaurant requires additional work to clearly distinguish each store and this improves readability and basic idea of store selling items.

5. ### Automated notifications about airflow dags:
   - Set automated notifications to slack channels and email updates on the data availability giving a visibility on the progress of the project and if there were any bugs through out the process.
  
This project was quickly implemented using modern AI assisted development workflows, Claude Code. The AI tools helped with:
- Debugging and error resolution
- Documentation drafting
