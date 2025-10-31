"""
RAG Service - Combines OpenAI and Pinecone for Retrieval-Augmented Generation.
Handles the full RAG pipeline: query -> embed -> search -> format.
"""

import logging
from typing import Optional
from .openai_service import OpenAIService
from .pinecone_service import PineconeService

logger = logging.getLogger(__name__)


class RAGService:
    """Service class for managing RAG (Retrieval-Augmented Generation) operations."""
    
    def __init__(
        self,
        openai_service: OpenAIService,
        pinecone_service: PineconeService
    ):
        """
        Initialize RAG service.
        
        Args:
            openai_service: OpenAI service instance
            pinecone_service: Pinecone service instance
        """
        self.openai_service = openai_service
        self.pinecone_service = pinecone_service
        logger.info("RAGService initialized")
    
    async def query_context(
        self,
        query_text: str,
        namespace: str,
        top_k: int = 5,
        embedding_model: str = "text-embedding-3-small"
    ) -> str:
        """
        Query Pinecone for relevant context using RAG.
        
        Args:
            query_text: The text to search for
            namespace: Pinecone namespace to search in
            top_k: Number of results to retrieve
            embedding_model: OpenAI embedding model to use
            
        Returns:
            Formatted context string from top results
        """
        try:
            logger.info(
                f"RAG Query - Namespace: {namespace}, "
                f"Query: {query_text[:100]}..."
            )
            
            # Step 1: Generate embedding for the query
            query_embedding = await self.openai_service.generate_embedding(
                text=query_text,
                model=embedding_model
            )
            
            # Step 2: Query Pinecone
            results = self.pinecone_service.query_vectors(
                vector=query_embedding,
                namespace=namespace,
                top_k=top_k,
                include_metadata=True
            )
            
            # Step 3: Format results
            if not results.matches:
                logger.info(f"No results found in namespace: {namespace}")
                return "No relevant context found."
            
            formatted_context = self.pinecone_service.format_search_results(
                results=results,
                max_results=top_k
            )
            
            logger.info(
                f"RAG Query completed - Retrieved {len(results.matches)} chunks"
            )
            
            return formatted_context
            
        except Exception as e:
            logger.error(f"Error in RAG query: {e}")
            return f"Error retrieving context: {str(e)}"
    
    async def query_with_filter(
        self,
        query_text: str,
        namespace: str,
        filter_dict: dict,
        top_k: int = 5
    ) -> str:
        """
        Query Pinecone with metadata filters.
        
        Args:
            query_text: The text to search for
            namespace: Pinecone namespace to search in
            filter_dict: Metadata filters to apply
            top_k: Number of results to retrieve
            
        Returns:
            Formatted context string
        """
        try:
            # Generate embedding
            query_embedding = await self.openai_service.generate_embedding(
                text=query_text
            )
            
            # Query with filter
            results = self.pinecone_service.query_vectors(
                vector=query_embedding,
                namespace=namespace,
                top_k=top_k,
                include_metadata=True,
                filter_dict=filter_dict
            )
            
            # Format results
            formatted_context = self.pinecone_service.format_search_results(
                results=results,
                max_results=top_k
            )
            
            logger.info(
                f"Filtered RAG Query completed - "
                f"Retrieved {len(results.matches) if results.matches else 0} chunks"
            )
            
            return formatted_context
            
        except Exception as e:
            logger.error(f"Error in filtered RAG query: {e}")
            return f"Error retrieving context: {str(e)}"
    
    def get_namespace_info(self, namespace: str) -> dict:
        """
        Get information about a specific namespace.
        
        Args:
            namespace: Namespace to get info for
            
        Returns:
            Dictionary with namespace information
        """
        try:
            stats = self.pinecone_service.get_index_stats()
            namespaces = stats.get('namespaces', {})
            
            if namespace in namespaces:
                ns_stats = namespaces[namespace]
                return {
                    'exists': True,
                    'vector_count': ns_stats.get('vector_count', 0),
                    'namespace': namespace
                }
            else:
                return {
                    'exists': False,
                    'namespace': namespace
                }
                
        except Exception as e:
            logger.error(f"Error getting namespace info: {e}")
            return {
                'exists': False,
                'namespace': namespace,
                'error': str(e)
            }

