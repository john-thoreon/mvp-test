"""
Pinecone Service - Handles all Pinecone vector database operations.
Includes vector search and namespace management.
"""

import logging
from typing import List, Dict, Any, Optional
from pinecone import Pinecone

logger = logging.getLogger(__name__)


class PineconeService:
    """Service class for managing Pinecone operations."""
    
    def __init__(self, api_key: str, index_name: str, environment: Optional[str] = None):
        """
        Initialize Pinecone service.
        
        Args:
            api_key: Pinecone API key
            index_name: Name of the Pinecone index
            environment: Pinecone environment (optional)
        """
        self.api_key = api_key
        self.index_name = index_name
        self.environment = environment
        self._client = None
        self._index = None
        logger.info(f"PineconeService initialized for index: {index_name}")
    
    @property
    def client(self) -> Pinecone:
        """Get or create Pinecone client (lazy initialization)."""
        if self._client is None:
            self._client = Pinecone(api_key=self.api_key)
            logger.info("Pinecone client created")
        return self._client
    
    @property
    def index(self):
        """Get or create Pinecone index connection (lazy initialization)."""
        if self._index is None:
            self._index = self.client.Index(self.index_name)
            logger.info(f"Connected to Pinecone index: {self.index_name}")
        return self._index
    
    def query_vectors(
        self,
        vector: List[float],
        namespace: str,
        top_k: int = 5,
        include_metadata: bool = True,
        filter_dict: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Query Pinecone for similar vectors.
        
        Args:
            vector: Query vector (embedding)
            namespace: Namespace to search in
            top_k: Number of results to return
            include_metadata: Whether to include metadata in results
            filter_dict: Optional metadata filters
            
        Returns:
            Query results
        """
        try:
            query_params = {
                'vector': vector,
                'top_k': top_k,
                'namespace': namespace,
                'include_metadata': include_metadata
            }
            
            if filter_dict:
                query_params['filter'] = filter_dict
            
            results = self.index.query(**query_params)
            
            logger.info(
                f"Query completed - Namespace: {namespace}, "
                f"Results: {len(results.matches) if results.matches else 0}"
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Error querying Pinecone: {e}")
            raise
    
    def format_search_results(
        self,
        results,
        max_results: Optional[int] = None
    ) -> str:
        """
        Format Pinecone search results into a readable string.
        
        Args:
            results: Pinecone query results
            max_results: Maximum number of results to format
            
        Returns:
            Formatted context string
        """
        try:
            if not results.matches:
                return "No relevant context found."
            
            matches = results.matches[:max_results] if max_results else results.matches
            context_parts = []
            
            for i, match in enumerate(matches, 1):
                score = match.score
                metadata = match.metadata or {}
                
                # Extract text content from metadata
                text = metadata.get('text', metadata.get('content', metadata.get('chunk', '')))
                source = metadata.get('source', 'Unknown')
                
                if text:
                    context_parts.append(
                        f"[Result {i} - Relevance: {score:.2f}, Source: {source}]\n{text}"
                    )
            
            if not context_parts:
                return "Context retrieved but no text content found."
            
            formatted_context = "\n\n".join(context_parts)
            logger.info(f"Formatted {len(context_parts)} results")
            
            return formatted_context
            
        except Exception as e:
            logger.error(f"Error formatting results: {e}")
            return f"Error formatting context: {str(e)}"
    
    def upsert_vectors(
        self,
        vectors: List[Dict[str, Any]],
        namespace: str
    ) -> Dict[str, Any]:
        """
        Insert or update vectors in Pinecone.
        
        Args:
            vectors: List of vector dictionaries with id, values, and metadata
            namespace: Namespace to upsert into
            
        Returns:
            Upsert response
        """
        try:
            response = self.index.upsert(
                vectors=vectors,
                namespace=namespace
            )
            
            logger.info(f"Upserted {len(vectors)} vectors to namespace: {namespace}")
            
            return {
                'success': True,
                'upserted_count': response.upserted_count if hasattr(response, 'upserted_count') else len(vectors),
                'namespace': namespace
            }
            
        except Exception as e:
            logger.error(f"Error upserting vectors: {e}")
            raise
    
    def delete_vectors(
        self,
        ids: Optional[List[str]] = None,
        namespace: str = "",
        delete_all: bool = False,
        filter_dict: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Delete vectors from Pinecone.
        
        Args:
            ids: List of vector IDs to delete
            namespace: Namespace to delete from
            delete_all: Whether to delete all vectors in namespace
            filter_dict: Optional metadata filter for deletion
            
        Returns:
            Deletion response
        """
        try:
            delete_params = {'namespace': namespace}
            
            if delete_all:
                delete_params['delete_all'] = True
            elif ids:
                delete_params['ids'] = ids
            elif filter_dict:
                delete_params['filter'] = filter_dict
            
            self.index.delete(**delete_params)
            
            logger.info(f"Deleted vectors from namespace: {namespace}")
            
            return {
                'success': True,
                'namespace': namespace
            }
            
        except Exception as e:
            logger.error(f"Error deleting vectors: {e}")
            raise
    
    def get_index_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the Pinecone index.
        
        Returns:
            Index statistics
        """
        try:
            stats = self.index.describe_index_stats()
            
            logger.info("Retrieved index statistics")
            
            return {
                'total_vector_count': stats.total_vector_count if hasattr(stats, 'total_vector_count') else 0,
                'dimension': stats.dimension if hasattr(stats, 'dimension') else 0,
                'namespaces': stats.namespaces if hasattr(stats, 'namespaces') else {}
            }
            
        except Exception as e:
            logger.error(f"Error getting index stats: {e}")
            raise
    
    def namespace_exists(self, namespace: str) -> bool:
        """
        Check if a namespace exists in the index.
        
        Args:
            namespace: Namespace to check
            
        Returns:
            True if namespace exists, False otherwise
        """
        try:
            stats = self.get_index_stats()
            namespaces = stats.get('namespaces', {})
            return namespace in namespaces
            
        except Exception as e:
            logger.error(f"Error checking namespace: {e}")
            return False

