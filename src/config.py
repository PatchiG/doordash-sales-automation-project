import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
OUTPUT_DIR = DATA_DIR / 'output'
LOGS_DIR = PROJECT_ROOT / 'logs'

# Ensure directories exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, OUTPUT_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# API Keys
GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

# Google Sheets Settings
GOOGLE_SHEETS_CREDENTIALS_PATH = PROJECT_ROOT / 'credentials.json'
GOOGLE_SHEETS_FOLDER_ID = os.getenv('GOOGLE_SHEETS_FOLDER_ID', None)  # Optional: folder to store sheets

# Data Collection Settings
CITIES = [
    'San Francisco, CA',
    'New York, NY',
    'Chicago, IL',
    'Los Angeles, CA',
    'Seattle, WA'
]

SEARCH_QUERIES = [
    'pizza restaurants',
    'chinese restaurants',
    'mexican restaurants',
    'grocery stores',
    'supermarkets',
    'convenience stores'
]

SEARCH_RADIUS = 50000  # 50km in meters

# Feature Engineering Settings
HIGH_DEMAND_CATEGORIES = [
    'pizza', 'chinese', 'mexican', 'sushi', 'thai', 'indian',
    'italian', 'japanese', 'korean', 'vietnamese',
    'grocery', 'supermarket', 'convenience'
]

# Lead Scoring Weights
SCORING_WEIGHTS = {
    'competitor_platform': 25,
    'review_count_high': 20,
    'review_count_medium': 15,
    'review_count_low': 10,
    'high_demand_category': 20,
    'urban_location': 15,
    'high_rating': 10,
    'affordable_price': 10
}

# Vertical-Specific Rules
VERTICAL_RULES = {
    'restaurants': {
        'min_score': 50,
        'target_count': 200,
        'sla_days': 7,
        'priority_categories': ['pizza', 'chinese', 'mexican']
    },
    'grocery': {
        'min_score': 60,
        'target_count': 100,
        'sla_days': 14,
        'priority_categories': ['grocery', 'supermarket']
    },
    'retail': {
        'min_score': 55,
        'target_count': 150,
        'sla_days': 10,
        'priority_categories': ['retail', 'shopping', 'convenience']
    }
}

# RAG System Settings
VECTOR_DB_PATH = PROJECT_ROOT / 'chroma_db'
EMBEDDING_MODEL = 'text-embedding-3-small'
LLM_MODEL = 'gpt-5-nano'  # Start with cheaper model
LLM_TEMPERATURE = 0.2
MAX_RETRIEVAL_DOCS = 5

# Streamlit Settings
STREAMLIT_PORT = 8501