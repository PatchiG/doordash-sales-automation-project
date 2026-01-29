"""
Vector Store Creation Module
Creates vector embeddings from scored leads data for semantic search
"""

from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document
import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv
import logging
import glob
import sys

sys.path.append(str(Path(__file__).parent.parent))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from src.config import PROCESSED_DATA_DIR, VECTOR_DB_PATH, EMBEDDING_MODEL


def load_latest_scored_leads():
    """
    Load the most recent scored leads file
    
    Returns:
        pandas.DataFrame: Scored leads data
    """
    
    pattern = str(PROCESSED_DATA_DIR / 'scored_leads_*.csv')
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"No scored leads found in {PROCESSED_DATA_DIR}")
    
    latest_file = max(files)
    logger.info(f"Loading scored leads from: {latest_file}")
    
    df = pd.read_csv(latest_file)
    logger.info(f"Loaded {len(df)} scored leads")
    
    return df


def create_documents_from_leads(df):
    """
    Convert leads dataframe into LangChain Document objects
    
    Args:
        df: pandas.DataFrame with scored leads
        
    Returns:
        list: List of Document objects
    """
    
    logger.info("Creating document representations for leads")
    
    documents = []
    
    for idx, row in df.iterrows():
        text_content = f"""
Business Name: {row['name']}
Category: {row.get('types', 'N/A')}
Location: {row['city']}, {row.get('state', '')}
Address: {row.get('address', 'N/A')}
Phone: {row.get('phone', 'N/A')}
Website: {row.get('website', 'N/A')}

Lead Score: {row['lead_score']}/100
Priority: {row['priority']}

Review Count: {row.get('review_count', 0)}
Average Rating: {row.get('rating', 0):.1f}/5.0

Competitor Platform Status:
- On Uber Eats: {'Yes' if row.get('on_ubereats', False) else 'No'}
- On Grubhub: {'Yes' if row.get('on_grubhub', False) else 'No'}

Contact Information:
- Contact By Date: {row.get('contact_by_date', 'N/A')}

Business Summary:
This is a {row['priority'].lower()} priority lead in the {row.get('types', 'general')} category located in {row['city']}. 
The business has {row.get('review_count', 0)} reviews with an average rating of {row.get('rating', 0):.1f} stars.
Lead score is {row['lead_score']}/100.
"""
        
        metadata = {
            'place_id': str(row.get('place_id', '')),
            'name': str(row['name']),
            'category': str(row.get('types', 'N/A')),
            'city': str(row['city']),
            'state': str(row.get('state', '')),
            'lead_score': int(row['lead_score']),
            'priority': str(row['priority']),
            'review_count': int(row.get('review_count', 0)),
            'rating': float(row.get('rating', 0)),
            'on_ubereats': bool(row.get('on_ubereats', False)),
            'on_grubhub': bool(row.get('on_grubhub', False)),
            'vertical': str(row.get('vertical', 'other'))
        }
        
        doc = Document(
            page_content=text_content,
            metadata=metadata
        )
        
        documents.append(doc)
        
        if (idx + 1) % 100 == 0:
            logger.info(f"  Prepared {idx + 1}/{len(df)} documents")
    
    logger.info(f"Created {len(documents)} document objects")
    
    return documents


def create_vector_store(documents):
    """
    Create vector store from documents using OpenAI embeddings
    
    Args:
        documents: list of Document objects
        
    Returns:
        Chroma: Vector store object
    """
    
    logger.info("Initializing OpenAI embeddings")
    logger.info(f"Using embedding model: {EMBEDDING_MODEL}")
    
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key:
        raise ValueError("OPENAI_API_KEY not found in environment variables")
    
    embeddings = OpenAIEmbeddings(
        model=EMBEDDING_MODEL,
        api_key=api_key
    )
    
    logger.info("Creating Chroma vector database")
    logger.info(f"Persist directory: {VECTOR_DB_PATH}")
    
    vectorstore = Chroma.from_documents(
        documents=documents,
        embedding=embeddings,
        persist_directory=str(VECTOR_DB_PATH)
    )

    logger.info("Vector store created and persisted successfully")
    
    return vectorstore


def calculate_embedding_cost(documents):
    """
    Estimate the cost of creating embeddings
    
    Args:
        documents: list of Document objects
        
    Returns:
        dict: Cost estimation details
    """
    
    total_text = ' '.join([doc.page_content for doc in documents])
    total_words = len(total_text.split())
    estimated_tokens = int(total_words * 1.3)
    
    cost_per_1k_tokens = 0.00002
    estimated_cost = (estimated_tokens / 1000) * cost_per_1k_tokens
    
    return {
        'total_documents': len(documents),
        'estimated_tokens': estimated_tokens,
        'estimated_cost_usd': estimated_cost
    }


def main():
    """Main execution function"""
    
    logger.info("="*60)
    logger.info("VECTOR STORE CREATION MODULE")
    logger.info("="*60)
    
    try:
        df = load_latest_scored_leads()
        
        documents = create_documents_from_leads(df)
        
        cost_info = calculate_embedding_cost(documents)
        logger.info("="*60)
        logger.info("COST ESTIMATION")
        logger.info("="*60)
        logger.info(f"Total Documents: {cost_info['total_documents']}")
        logger.info(f"Estimated Tokens: {cost_info['estimated_tokens']:,}")
        logger.info(f"Estimated Cost: ${cost_info['estimated_cost_usd']:.4f}")
        logger.info("="*60)
        
        vectorstore = create_vector_store(documents)
        
        collection_size = vectorstore._collection.count()
        logger.info(f"Vector store contains {collection_size} documents")
        
        logger.info("="*60)
        logger.info("Vector store creation completed successfully")
        logger.info(f"Saved to: {VECTOR_DB_PATH}")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Vector store creation failed: {e}")
        raise


if __name__ == "__main__":
    main()