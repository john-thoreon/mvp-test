"""
Twilio Service - Handles all Twilio-related operations.
Includes call management, TwiML generation, and status tracking.
"""

import logging
from typing import Dict, Any, Optional
from twilio.rest import Client as TwilioClient
from twilio.twiml.voice_response import VoiceResponse, Connect, Stream

logger = logging.getLogger(__name__)


class TwilioService:
    """Service class for managing Twilio operations."""
    
    def __init__(self, account_sid: str, auth_token: str, phone_number: str):
        """
        Initialize Twilio service.
        
        Args:
            account_sid: Twilio account SID
            auth_token: Twilio auth token
            phone_number: Twilio phone number for outbound calls
        """
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.phone_number = phone_number
        self._client = None
        logger.info("TwilioService initialized")
    
    @property
    def client(self) -> TwilioClient:
        """Get or create Twilio client (lazy initialization)."""
        if self._client is None:
            self._client = TwilioClient(self.account_sid, self.auth_token)
            logger.info("Twilio client created")
        return self._client
    
    def generate_interactive_twiml(self, websocket_url: str, namespace: str) -> str:
        """
        Generate TwiML for interactive AI call with WebSocket streaming.
        
        Args:
            websocket_url: WebSocket endpoint URL for media streaming
            namespace: Pinecone namespace to use for context
            
        Returns:
            TwiML XML string
        """
        response = VoiceResponse()
        response.say("Connecting you to an AI assistant. Please wait.", voice='alice')
        
        # Start WebSocket stream
        connect = Connect()
        stream = Stream(url=f"{websocket_url}?namespace={namespace}")
        connect.append(stream)
        response.append(connect)
        
        return str(response)
    
    def generate_alert_twiml(self, message: str, voice: str = 'alice', language: str = 'en-US') -> str:
        """
        Generate TwiML for simple alert message.
        
        Args:
            message: Text message to speak
            voice: Voice to use for text-to-speech
            language: Language code
            
        Returns:
            TwiML XML string
        """
        response = VoiceResponse()
        response.say(message, voice=voice, language=language)
        response.hangup()
        
        return str(response)
    
    def initiate_call(
        self,
        to_number: str,
        twiml: str,
        status_callback_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Initiate an outbound call.
        
        Args:
            to_number: Phone number to call (E.164 format)
            twiml: TwiML instructions for the call
            status_callback_url: URL for status callbacks
            
        Returns:
            Dictionary with call details
        """
        try:
            call_params = {
                'to': to_number,
                'from_': self.phone_number,
                'twiml': twiml
            }
            
            if status_callback_url:
                call_params.update({
                    'status_callback': status_callback_url,
                    'status_callback_event': ['initiated', 'ringing', 'answered', 'completed'],
                    'status_callback_method': 'POST'
                })
            
            call = self.client.calls.create(**call_params)
            
            logger.info(f"Call initiated - SID: {call.sid}, To: {to_number}, Status: {call.status}")
            
            return {
                'call_sid': call.sid,
                'status': call.status,
                'to': to_number,
                'from': self.phone_number,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error initiating call: {e}")
            raise
    
    def get_call_status(self, call_sid: str) -> Dict[str, Any]:
        """
        Get the status of a specific call.
        
        Args:
            call_sid: Twilio call SID
            
        Returns:
            Dictionary with call status information
        """
        try:
            call = self.client.calls(call_sid).fetch()
            
            # Safely get from and to numbers - 'from' is a Python keyword so we use getattr
            from_number = 'Unknown'
            to_number = 'Unknown'
            
            # Try different attribute names using getattr to avoid Python keyword issues
            if hasattr(call, 'from_formatted'):
                from_number = call.from_formatted
            else:
                # Use getattr for 'from' since it's a reserved keyword
                from_number = getattr(call, 'from', 'Unknown')
            
            if hasattr(call, 'to_formatted'):
                to_number = call.to_formatted
            else:
                to_number = getattr(call, 'to', 'Unknown')
            
            return {
                'call_sid': call.sid,
                'status': call.status,
                'direction': call.direction if hasattr(call, 'direction') else 'Unknown',
                'from': from_number,
                'to': to_number,
                'duration': call.duration if hasattr(call, 'duration') else None,
                'start_time': call.start_time.isoformat() if hasattr(call, 'start_time') and call.start_time else None,
                'end_time': call.end_time.isoformat() if hasattr(call, 'end_time') and call.end_time else None,
                'price': call.price if hasattr(call, 'price') else None,
                'price_unit': call.price_unit if hasattr(call, 'price_unit') else None
            }
            
        except Exception as e:
            logger.error(f"Error fetching call status for {call_sid}: {e}")
            raise
    
    def validate_phone_number(self, phone_number: str) -> bool:
        """
        Validate phone number format (E.164).
        
        Args:
            phone_number: Phone number to validate
            
        Returns:
            True if valid, False otherwise
        """
        return phone_number.startswith('+') and len(phone_number) > 10
    
    def update_call(self, call_sid: str, **kwargs) -> Dict[str, Any]:
        """
        Update an active call.
        
        Args:
            call_sid: Twilio call SID
            **kwargs: Parameters to update (e.g., status='completed' to end call)
            
        Returns:
            Updated call information
        """
        try:
            call = self.client.calls(call_sid).update(**kwargs)
            
            logger.info(f"Call updated - SID: {call_sid}, Status: {call.status}")
            
            return {
                'call_sid': call.sid,
                'status': call.status,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Error updating call {call_sid}: {e}")
            raise

