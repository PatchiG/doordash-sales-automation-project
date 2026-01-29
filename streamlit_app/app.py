"""
Streamlit Application
Sales Intelligence Assistant Interface
"""

import streamlit as st
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from rag_system.query_engine import SalesIntelligenceRAG

st.set_page_config(
    page_title="DoorDash Sales Intelligence",
    page_icon="🎯",
    layout="wide"
)


@st.cache_resource
def load_rag_system():
    """Load and cache the RAG system"""
    return SalesIntelligenceRAG()


def main():
    """Main application"""
    
    st.title("DoorDash Sales Intelligence Assistant")
    st.caption("AI-powered lead search and analysis system")
    
    try:
        with st.spinner("Loading AI system..."):
            rag = load_rag_system()
        
        if 'messages' not in st.session_state:
            st.session_state.messages = []

        if 'pending_query' not in st.session_state:
            st.session_state.pending_query = None

        # Process pending query from sidebar buttons
        if st.session_state.pending_query:
            query = st.session_state.pending_query
            st.session_state.pending_query = None

            with st.chat_message("user"):
                st.markdown(query)

            with st.chat_message("assistant"):
                with st.spinner("Searching..."):
                    result = rag.query(query)
                    st.markdown(result['answer'])

                    if result['sources']:
                        with st.expander("View Sources"):
                            for i, source in enumerate(result['sources'], 1):
                                st.write(f"**{i}. {source['name']}**")
                                st.write(f"Category: {source['category']}")
                                st.write(f"Location: {source['city']}")
                                st.write(f"Lead Score: {source['lead_score']}/100")
                                st.write(f"Rating: {source['rating']}/5.0 ({source['review_count']} reviews)")
                                st.divider()

            st.session_state.messages.append({"role": "user", "content": query})
            st.session_state.messages.append({
                "role": "assistant",
                "content": result['answer'],
                "sources": result['sources']
            })

        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
                
                if "sources" in message and message["sources"]:
                    with st.expander("View Sources"):
                        for i, source in enumerate(message["sources"], 1):
                            st.write(f"**{i}. {source['name']}**")
                            st.write(f"Category: {source['category']}")
                            st.write(f"Location: {source['city']}")
                            st.write(f"Lead Score: {source['lead_score']}/100")
                            st.write(f"Rating: {source['rating']}/5.0 ({source['review_count']} reviews)")
                            st.divider()
        
        if prompt := st.chat_input("Ask about leads..."):
            st.session_state.messages.append({"role": "user", "content": prompt})
            
            with st.chat_message("user"):
                st.markdown(prompt)
            
            with st.chat_message("assistant"):
                with st.spinner("Searching..."):
                    result = rag.query(prompt)
                    
                    st.markdown(result['answer'])
                    
                    if result['sources']:
                        with st.expander("View Sources"):
                            for i, source in enumerate(result['sources'], 1):
                                st.write(f"**{i}. {source['name']}**")
                                st.write(f"Category: {source['category']}")
                                st.write(f"Location: {source['city']}")
                                st.write(f"Lead Score: {source['lead_score']}/100")
                                st.write(f"Rating: {source['rating']}/5.0 ({source['review_count']} reviews)")
                                st.divider()
            
            st.session_state.messages.append({
                "role": "assistant",
                "content": result['answer'],
                "sources": result['sources']
            })
        
        with st.sidebar:
            st.header("Example Questions")
            
            examples = [
                "Show me pizza restaurants in San Francisco",
                "Find high-priority leads",
                "Which leads are on Uber Eats?",
                "Show me grocery stores with high ratings",
                "Find leads in New York"
            ]
            
            for example in examples:
                if st.button(example, key=example):
                    st.session_state.pending_query = example
                    st.rerun()
            
            st.divider()
            st.info(f"Embedding Model: {rag.embeddings.model}")
            st.info(f"LLM: {rag.llm.model_name}")
            st.success("System Status: Active")
    
    except Exception as e:
        st.error(f"Error: {str(e)}")
        st.info("Please ensure the vector store has been created by running: python rag_system/create_vectorstore.py")


if __name__ == "__main__":
    main()