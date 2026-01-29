"""
Debug RAG system to identify issues
"""

import sys
from pathlib import Path
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

sys.path.append(str(Path(__file__).parent))

def test_imports():
    """Test all required imports"""
    logger.info("Testing imports...")
    
    try:
        from langchain.embeddings import OpenAIEmbeddings
        logger.info("  langchain.embeddings: OK")
    except Exception as e:
        logger.error(f"  langchain.embeddings: FAILED - {e}")
        return False
    
    try:
        from langchain.vectorstores import Chroma
        logger.info("  langchain.vectorstores: OK")
    except Exception as e:
        logger.error(f"  langchain.vectorstores: FAILED - {e}")
        return False
    
    try:
        from langchain.chat_models import ChatOpenAI
        logger.info("  langchain.chat_models: OK")
    except Exception as e:
        logger.error(f"  langchain.chat_models: FAILED - {e}")
        return False
    
    try:
        from langchain.chains import RetrievalQA
        logger.info("  langchain.chains: OK")
    except Exception as e:
        logger.error(f"  langchain.chains: FAILED - {e}")
        return False
    
    return True


def test_config():
    """Test config loading"""
    logger.info("Testing config...")
    
    try:
        from src.config import VECTOR_DB_PATH, EMBEDDING_MODEL, LLM_MODEL
        logger.info(f"  VECTOR_DB_PATH: {VECTOR_DB_PATH}")
        logger.info(f"  EMBEDDING_MODEL: {EMBEDDING_MODEL}")
        logger.info(f"  LLM_MODEL: {LLM_MODEL}")
        
        if not VECTOR_DB_PATH.exists():
            logger.error(f"  Vector DB path does not exist: {VECTOR_DB_PATH}")
            return False
        
        return True
    except Exception as e:
        logger.error(f"  Config loading failed: {e}")
        return False


def test_openai_connection():
    """Test OpenAI API connection"""
    logger.info("Testing OpenAI connection...")
    
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key:
        logger.error("  OPENAI_API_KEY not found in environment")
        return False
    
    logger.info(f"  API Key found: {api_key[:10]}...")
    
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Test"}],
            max_tokens=5
        )
        
        logger.info("  OpenAI API: OK")
        return True
    except Exception as e:
        logger.error(f"  OpenAI API: FAILED - {e}")
        return False


def test_vector_store_loading():
    """Test loading vector store"""
    logger.info("Testing vector store loading...")
    
    try:
        from langchain.embeddings import OpenAIEmbeddings
        from langchain.vectorstores import Chroma
        from src.config import VECTOR_DB_PATH, EMBEDDING_MODEL
        import os
        
        api_key = os.getenv('OPENAI_API_KEY')
        
        embeddings = OpenAIEmbeddings(
            model=EMBEDDING_MODEL,
            openai_api_key=api_key
        )
        
        vectorstore = Chroma(
            persist_directory=str(VECTOR_DB_PATH),
            embedding_function=embeddings
        )
        
        count = vectorstore._collection.count()
        logger.info(f"  Vector store loaded: {count} documents")
        
        if count == 0:
            logger.error("  Vector store is empty!")
            return False
        
        return True
    except Exception as e:
        logger.error(f"  Vector store loading failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def test_simple_query():
    """Test a simple query"""
    logger.info("Testing simple query...")
    
    try:
        from rag_system.query_engine import SalesIntelligenceRAG
        
        rag = SalesIntelligenceRAG()
        
        result = rag.query("Show me pizza restaurants in San Francisco")
        
        logger.info(f"  Query successful!")
        logger.info(f"  Answer: {result['answer'][:100]}...")
        logger.info(f"  Sources: {len(result['sources'])}")
        
        return True
    except Exception as e:
        logger.error(f"  Query failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Run all tests"""
    
    logger.info("="*60)
    logger.info("RAG SYSTEM DEBUG")
    logger.info("="*60)
    
    tests = [
        ("Imports", test_imports),
        ("Config", test_config),
        ("OpenAI Connection", test_openai_connection),
        ("Vector Store Loading", test_vector_store_loading),
        ("Simple Query", test_simple_query)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\nTest: {test_name}")
        logger.info("-"*60)
        
        try:
            result = test_func()
            results[test_name] = result
        except Exception as e:
            logger.error(f"Test crashed: {e}")
            results[test_name] = False
            import traceback
            logger.error(traceback.format_exc())
    
    logger.info("\n" + "="*60)
    logger.info("TEST RESULTS")
    logger.info("="*60)
    
    for test_name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        logger.info(f"{status:8s} {test_name}")
    
    logger.info("="*60)
    
    if all(results.values()):
        logger.info("All tests passed - RAG system is working")
    else:
        logger.error("Some tests failed - see errors above")


if __name__ == "__main__":
    main()