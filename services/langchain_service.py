"""
LangChain-based service for context-aware question answering.
This handles intelligent extraction of information from structured context.
"""

import os
import logging
from typing import Optional, Dict, Any
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser

logger = logging.getLogger(__name__)


class LangChainContextService:
    """Service for using LangChain to answer questions from context"""
    
    def __init__(self, openai_api_key: Optional[str] = None):
        """
        Initialize the LangChain service.
        
        Args:
            openai_api_key: OpenAI API key (defaults to environment variable)
        """
        self.api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key is required")
        
        # Initialize the LLM with GPT-4o for best performance
        self.llm = ChatOpenAI(
            model="gpt-4o",
            temperature=0.1,  # Low temperature for precise, factual answers
            api_key=self.api_key
        )
        
        # Create a prompt template for context-based QA
        self.qa_template = PromptTemplate(
            input_variables=["context", "question"],
            template="""You are a precise data analysis assistant. You have access to structured data and must answer questions ONLY based on the provided context.

CONTEXT:
{context}

QUESTION: {question}

INSTRUCTIONS:
1. Analyze the context carefully to find the requested information
2. Extract exact numbers, names, and details from the context
3. Be specific and cite the relevant parts of the data
4. If the answer requires summarizing multiple items, provide a clear breakdown
5c. If the information is not in the context, say "I don't have that specific information in the provided data."
6. Present information in a clear, conversational manner suitable for voice output
7. Keep responses concise but complete - aim for 2-4 sentences for summaries

ANSWER:"""
        )
        
        # Create the chain
        self.qa_chain = self.qa_template | self.llm | StrOutputParser()
        
        logger.info("LangChain Context Service initialized with GPT-4o")
    
    async def answer_question(self, question: str, context: str) -> str:
        """
        Answer a question using the provided context.
        
        Args:
            question: The user's question
            context: The context data to search through
            
        Returns:
            str: The answer to the question
        """
        try:
            logger.info(f"LangChain processing question: {question[:100]}...")
            
            # Invoke the chain
            answer = await self.qa_chain.ainvoke({
                "context": context,
                "question": question
            })
            
            logger.info(f"LangChain answer: {answer[:200]}...")
            return answer.strip()
            
        except Exception as e:
            logger.error(f"Error in LangChain QA: {e}")
            return f"I encountered an error processing your question: {str(e)}"
    
    def answer_question_sync(self, question: str, context: str) -> str:
        """
        Synchronous version of answer_question.
        
        Args:
            question: The user's question
            context: The context data to search through
            
        Returns:
            str: The answer to the question
        """
        try:
            logger.info(f"LangChain processing question (sync): {question[:100]}...")
            
            # Invoke the chain synchronously
            answer = self.qa_chain.invoke({
                "context": context,
                "question": question
            })
            
            logger.info(f"LangChain answer: {answer[:200]}...")
            return answer.strip()
            
        except Exception as e:
            logger.error(f"Error in LangChain QA (sync): {e}")
            return f"I encountered an error processing your question: {str(e)}"
    
    async def generate_summary(self, context: str, aspect: str = "overall") -> str:
        """
        Generate a summary of the context.
        
        Args:
            context: The context data
            aspect: The aspect to summarize (overall, by_site, by_type, by_subject)
            
        Returns:
            str: The summary
        """
        questions = {
            "overall": "Provide a high-level summary of the key statistics and totals from this data.",
            "by_site": "Summarize the data broken down by site, including site-specific counts and issues.",
            "by_type": "Summarize the data broken down by type or category.",
            "by_subject": "Summarize which subjects are affected and how many issues each has."
        }
        
        question = questions.get(aspect, questions["overall"])
        return await self.answer_question(question, context)


# Singleton instance
_langchain_service: Optional[LangChainContextService] = None


def get_langchain_service() -> LangChainContextService:
    """Get or create the LangChain service instance"""
    global _langchain_service
    if _langchain_service is None:
        _langchain_service = LangChainContextService()
    return _langchain_service

