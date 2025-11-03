"""
OpenAI Service - Handles all OpenAI-related operations.
Includes embeddings generation and Real-time API management.
"""

import logging
import json
import asyncio
from typing import List, Dict, Any, Optional
from openai import OpenAI, AsyncOpenAI
import websockets

logger = logging.getLogger(__name__)


class OpenAIService:
    """Service class for managing OpenAI operations."""
    
    def __init__(self, api_key: str):
        """
        Initialize OpenAI service.
        
        Args:
            api_key: OpenAI API key
        """
        self.api_key = api_key
        self._client = None
        self._async_client = None
        logger.info("OpenAIService initialized")
    
    @property
    def client(self) -> OpenAI:
        """Get or create synchronous OpenAI client (lazy initialization)."""
        if self._client is None:
            self._client = OpenAI(api_key=self.api_key)
            logger.info("OpenAI sync client created")
        return self._client
    
    @property
    def async_client(self) -> AsyncOpenAI:
        """Get or create asynchronous OpenAI client (lazy initialization)."""
        if self._async_client is None:
            self._async_client = AsyncOpenAI(api_key=self.api_key)
            logger.info("OpenAI async client created")
        return self._async_client
    
    async def generate_embedding(
        self,
        text: str,
        model: str = "text-embedding-3-large"
    ) -> List[float]:
        """
        Generate embedding for text using OpenAI.
        
        Args:
            text: Text to embed
            model: Embedding model to use
            
        Returns:
            List of floats representing the embedding
        """
        try:
            response = await self.async_client.embeddings.create(
                model=model,
                input=text
            )
            embedding = response.data[0].embedding
            logger.info(f"Generated embedding for text (length: {len(text)})")
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise
    
    async def connect_realtime_api(
        self,
        session_config: Optional[Dict[str, Any]] = None
    ) -> websockets.WebSocketClientProtocol:
        """
        Connect to OpenAI Real-time API via WebSocket.
        
        Args:
            session_config: Optional session configuration
            
        Returns:
            WebSocket connection
        """
        try:
            ws_url = "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01"
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "OpenAI-Beta": "realtime=v1"
            }
            
            logger.info("Connecting to OpenAI Real-time API...")
            ws = await websockets.connect(ws_url, additional_headers=headers)
            logger.info("Connected to OpenAI Real-time API")
            
            # Send session configuration if provided
            if session_config:
                await ws.send(json.dumps(session_config))
                logger.info("Session configuration sent")
            
            return ws
            
        except Exception as e:
            logger.error(f"Error connecting to Real-time API: {e}")
            raise
    
    def get_default_session_config(
        self,
        instructions: str = "You are a helpful AI assistant.",
        voice: str = "alloy",
        temperature: float = 0.7
    ) -> Dict[str, Any]:
        """
        Get default session configuration for Real-time API.
        
        Args:
            instructions: System instructions for the AI
            voice: Voice to use (alloy, echo, fable, onyx, nova, shimmer)
            temperature: Temperature for response generation
            
        Returns:
            Session configuration dictionary
        """
        return {
            "type": "session.update",
            "session": {
                "modalities": ["text", "audio"],
                "instructions": instructions,
                "voice": voice,
                "input_audio_format": "pcm16",
                "output_audio_format": "pcm16",
                "input_audio_transcription": {
                    "model": "whisper-1"
                },
                "turn_detection": {
                    "type": "server_vad",
                    "threshold": 0.5,
                    "prefix_padding_ms": 300,
                    "silence_duration_ms": 500
                },
                "temperature": temperature,
                "max_response_output_tokens": 4096
            }
        }
    
    async def send_audio_to_realtime(
        self,
        ws: websockets.WebSocketClientProtocol,
        audio_data: str
    ):
        """
        Send audio data to OpenAI Real-time API.
        
        Args:
            ws: WebSocket connection
            audio_data: Base64-encoded audio data
        """
        try:
            message = {
                "type": "input_audio_buffer.append",
                "audio": audio_data
            }
            await ws.send(json.dumps(message))
            
        except Exception as e:
            logger.error(f"Error sending audio to Real-time API: {e}")
            raise
    
    async def inject_context_to_conversation(
        self,
        ws: websockets.WebSocketClientProtocol,
        context: str
    ):
        """
        Inject context into the conversation.
        
        Args:
            ws: WebSocket connection
            context: Context text to inject
        """
        try:
            context_message = {
                "type": "conversation.item.create",
                "item": {
                    "type": "message",
                    "role": "system",
                    "content": [
                        {
                            "type": "input_text",
                            "text": f"Context from knowledge base:\n\n{context}"
                        }
                    ]
                }
            }
            await ws.send(json.dumps(context_message))
            logger.info("Context injected into conversation")
            
        except Exception as e:
            logger.error(f"Error injecting context: {e}")
            raise
    
    async def handle_realtime_event(
        self,
        event: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Handle an event from OpenAI Real-time API.
        
        Args:
            event: Event dictionary from Real-time API
            
        Returns:
            Processed event data or None
        """
        event_type = event.get('type')
        
        if event_type == 'response.audio.delta':
            # Audio response chunk
            return {
                'type': 'audio',
                'audio_delta': event.get('delta')
            }
            
        elif event_type == 'conversation.item.input_audio_transcription.completed':
            # User speech transcription
            transcript = event.get('transcript', '')
            logger.info(f"Transcribed: {transcript}")
            return {
                'type': 'transcript',
                'text': transcript
            }
            
        elif event_type == 'response.done':
            logger.info("Response completed")
            return {
                'type': 'response_done'
            }
            
        elif event_type == 'error':
            error_info = event.get('error', {})
            logger.error(f"OpenAI error: {error_info}")
            return {
                'type': 'error',
                'error': error_info
            }
        
        return None

