"""
Query Engine Module
Handles RAG-based querying of the sales intelligence system
"""

from langchain_classic.chains import RetrievalQA
from langchain_core.prompts import PromptTemplate
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_chroma import Chroma
import os
from dotenv import load_dotenv
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from src.config import VECTOR_DB_PATH, EMBEDDING_MODEL, LLM_MODEL, LLM_TEMPERATURE, MAX_RETRIEVAL_DOCS


class SalesIntelligenceRAG:
    """
    RAG-based sales intelligence query system
    """
    
    def __init__(self):
        """Initialize the RAG system"""
        
        logger.info("Initializing Sales Intelligence RAG system")
        
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables")
        
        logger.info(f"Loading embeddings model: {EMBEDDING_MODEL}")
        self.embeddings = OpenAIEmbeddings(
            model=EMBEDDING_MODEL,
            api_key=api_key
        )
        
        logger.info(f"Loading vector store from: {VECTOR_DB_PATH}")
        self.vectorstore = Chroma(
            persist_directory=str(VECTOR_DB_PATH),
            embedding_function=self.embeddings
        )
        
        logger.info(f"Initializing LLM: {LLM_MODEL}")
        self.llm = ChatOpenAI(
            model=LLM_MODEL,
            temperature=LLM_TEMPERATURE,
            api_key=api_key
        )
        
        self.qa_chain = self._create_qa_chain()
        
        logger.info("RAG system initialized successfully")
    
    def _create_qa_chain(self):
        """
        Create the RetrievalQA chain with custom prompt
        
        Returns:
            RetrievalQA: Configured QA chain
        """
        
        prompt_template = """You are a sales intelligence assistant for DoorDash's GTM team.
Your role is to help sales representatives find and understand merchant leads.

Use the following context about merchant leads to answer the question.
Be specific and cite the lead names in your answer.
If you cannot find relevant information in the context, say so clearly.
Do not make up information.

Context: {context}

Question: {question}

Answer:"""
        
        prompt = PromptTemplate(
            template=prompt_template,
            input_variables=["context", "question"]
        )
        
        qa_chain = RetrievalQA.from_chain_type(
            llm=self.llm,
            chain_type="stuff",
            retriever=self.vectorstore.as_retriever(
                search_kwargs={"k": MAX_RETRIEVAL_DOCS}
            ),
            chain_type_kwargs={"prompt": prompt},
            return_source_documents=True
        )
        
        return qa_chain
    
    def query(self, question):
        """
        Query the RAG system
        
        Args:
            question: str, user question
            
        Returns:
            dict: Answer and source documents
        """
        
        logger.info(f"Processing query: {question}")
        
        result = self.qa_chain({"query": question})
        
        answer = result['result']
        source_docs = result['source_documents']
        
        sources = []
        for doc in source_docs:
            sources.append({
                'name': doc.metadata.get('name'),
                'category': doc.metadata.get('category'),
                'city': doc.metadata.get('city'),
                'lead_score': doc.metadata.get('lead_score'),
                'priority': doc.metadata.get('priority'),
                'rating': doc.metadata.get('rating'),
                'review_count': doc.metadata.get('review_count')
            })
        
        logger.info(f"Retrieved {len(sources)} source documents")
        
        return {
            'answer': answer,
            'sources': sources
        }


def test_rag_system():
    """Test the RAG system with sample queries"""
    
    logger.info("="*60)
    logger.info("TESTING RAG SYSTEM")
    logger.info("="*60)
    
    rag = SalesIntelligenceRAG()
    
    test_queries = [
        "Show me the top 5 pizza restaurants in San Francisco",
        "Which leads are on competitor platforms like Uber Eats or Grubhub?",
        "Find high-priority leads with good ratings",
        "What are the best grocery store leads?",
        "Show me leads in New York that need to be contacted soon"
    ]
    
    for query in test_queries:
        logger.info(f"\nQuery: {query}")
        logger.info("-"*60)
        
        result = rag.query(query)
        
        logger.info(f"Answer: {result['answer']}")
        
        logger.info("\nSources:")
        for i, source in enumerate(result['sources'][:3], 1):
            logger.info(f"  {i}. {source['name']} ({source['category']})")
            logger.info(f"     City: {source['city']}, Score: {source['lead_score']}, Rating: {source['rating']}")
        
        logger.info("")


def main():
    """Main execution function"""
    
    try:
        test_rag_system()
        
        logger.info("="*60)
        logger.info("RAG system testing completed successfully")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"RAG system test failed: {e}")
        raise


if __name__ == "__main__":
    main()