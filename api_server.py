from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Depends, Form, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse, Response, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import os
import time
import tempfile
import json
import logging
import threading
import base64
import asyncio
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import pubsub_v1, storage
from google.api_core import retry
from google.oauth2 import service_account
from unstructured_client import UnstructuredClient
from unstructured_client.models.operations import (
    ListWorkflowsRequest, ListJobsRequest, RunWorkflowRequest, 
    ListDestinationsRequest, UpdateDestinationRequest
)
from unstructured_client.models.shared import UpdateDestinationConnector
import websockets

# Import our service classes
from services import TwilioService, OpenAIService, PineconeService, RAGService

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="PDF Upload & Workflow API",
    description="REST API for uploading PDFs and triggering Unstructured workflows",
    version="1.0.0"
)

# Mount static files directory for test page
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# GCP Pub/Sub configuration
PUBSUB_PROJECT_ID = os.getenv("PUBSUB_PROJECT_ID")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "workflow-jobs")
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION", "workflow-jobs-sub")
PUBSUB_CREDENTIALS_PATH = os.getenv("GCP_PUB_SUB_CREDENTIALS")
PUBSUB_CREDENTIALS_BASE64 = os.getenv("GCP_PUB_SUB_CREDENTIALS_BASE64")

# Twilio configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")

# OpenAI configuration (for Real-time API and embeddings)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Pinecone configuration
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")
PINECONE_ENVIRONMENT = os.getenv("PINECONE_ENVIRONMENT")

# Global job state - thread safe
current_job_lock = threading.Lock()
current_job_id = None
job_queue_count = 0

# Pydantic models for request/response
class WorkflowInfo(BaseModel):
    id: str
    name: str
    status: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class UploadResponse(BaseModel):
    success: bool
    filename: str
    gcs_path: str
    bucket: str
    size: int
    job_id: Optional[str] = None
    workflow_id: str
    namespace: str
    message: str

class JobStatus(BaseModel):
    job_id: str
    status: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    workflow_name: Optional[str] = None

class ApiError(BaseModel):
    error: str
    details: Optional[str] = None

class CallRequest(BaseModel):
    phone_number: str
    namespace: Optional[str] = None
    context_text: Optional[str] = None
    initial_message: Optional[str] = "Hello, how can I help you today?"

class AlertCallRequest(BaseModel):
    phone_number: str
    message: str

class CallResponse(BaseModel):
    success: bool
    call_sid: str
    phone_number: str
    status: str
    message: str
    namespace: Optional[str] = None

# Cached clients (existing GCP and Unstructured)
_unstructured_client = None
_gcs_client = None
_pubsub_publisher = None
_pubsub_subscriber = None

# Cached service instances (new OOP approach)
_twilio_service = None
_openai_service = None
_pinecone_service = None
_rag_service = None

def get_unstructured_client():
    """Get or initialize Unstructured client"""
    global _unstructured_client
    if _unstructured_client is None:
        api_key = os.getenv("UNSTRUCTURED_API_KEY")
        if not api_key:
            raise HTTPException(status_code=500, detail="UNSTRUCTURED_API_KEY not found")
        _unstructured_client = UnstructuredClient(api_key_auth=api_key)
    return _unstructured_client

def get_gcs_client():
    """Get or initialize GCS client"""
    global _gcs_client
    if _gcs_client is None:
        try:
            project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
            
            # Priority 1: Check for base64-encoded credentials
            credentials_base64 = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_BASE64")
            if credentials_base64:
                credentials_json = base64.b64decode(credentials_base64).decode('utf-8')
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                _gcs_client = storage.Client(credentials=credentials, project=project_id)
            # Priority 2: Check for file path
            elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
                credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                if os.path.exists(credentials_path):
                    credentials = service_account.Credentials.from_service_account_file(credentials_path)
                    _gcs_client = storage.Client(credentials=credentials, project=project_id)
            # Priority 3: Use default credentials
            else:
                _gcs_client = storage.Client(project=project_id)
                
        except Exception as e:
            logger.error(f"Error initializing GCS client: {e}")
            raise HTTPException(status_code=500, detail=f"GCS initialization failed: {e}")
    
    return _gcs_client

def get_pubsub_publisher():
    """Get or initialize Pub/Sub publisher"""
    global _pubsub_publisher
    if _pubsub_publisher is None:
        try:
            if PUBSUB_CREDENTIALS_BASE64:
                credentials_json = base64.b64decode(PUBSUB_CREDENTIALS_BASE64).decode('utf-8')
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                _pubsub_publisher = pubsub_v1.PublisherClient(credentials=credentials)
            elif PUBSUB_CREDENTIALS_PATH and os.path.exists(PUBSUB_CREDENTIALS_PATH):
                credentials = service_account.Credentials.from_service_account_file(PUBSUB_CREDENTIALS_PATH)
                _pubsub_publisher = pubsub_v1.PublisherClient(credentials=credentials)
            else:
                _pubsub_publisher = pubsub_v1.PublisherClient()
        except Exception as e:
            logger.error(f"Error initializing Pub/Sub publisher: {e}")
            raise HTTPException(status_code=500, detail=f"Pub/Sub initialization failed: {e}")
    
    return _pubsub_publisher

def get_twilio_service() -> TwilioService:
    """Get or initialize Twilio service"""
    global _twilio_service
    if _twilio_service is None:
        try:
            if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN or not TWILIO_PHONE_NUMBER:
                raise HTTPException(
                    status_code=500,
                    detail="Twilio credentials not configured (TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER)"
                )
            _twilio_service = TwilioService(
                account_sid=TWILIO_ACCOUNT_SID,
                auth_token=TWILIO_AUTH_TOKEN,
                phone_number=TWILIO_PHONE_NUMBER
            )
            logger.info("Twilio service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Twilio service: {e}")
            raise HTTPException(status_code=500, detail=f"Twilio initialization failed: {e}")
    
    return _twilio_service

def get_openai_service() -> OpenAIService:
    """Get or initialize OpenAI service"""
    global _openai_service
    if _openai_service is None:
        try:
            if not OPENAI_API_KEY:
                raise HTTPException(status_code=500, detail="OPENAI_API_KEY not found")
            _openai_service = OpenAIService(api_key=OPENAI_API_KEY)
            logger.info("OpenAI service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing OpenAI service: {e}")
            raise HTTPException(status_code=500, detail=f"OpenAI initialization failed: {e}")
    
    return _openai_service

def get_pinecone_service() -> PineconeService:
    """Get or initialize Pinecone service"""
    global _pinecone_service
    if _pinecone_service is None:
        try:
            if not PINECONE_API_KEY or not PINECONE_INDEX_NAME:
                raise HTTPException(
                    status_code=500,
                    detail="Pinecone credentials not configured (PINECONE_API_KEY, PINECONE_INDEX_NAME)"
                )
            _pinecone_service = PineconeService(
                api_key=PINECONE_API_KEY,
                index_name=PINECONE_INDEX_NAME,
                environment=PINECONE_ENVIRONMENT
            )
            logger.info("Pinecone service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing Pinecone service: {e}")
            raise HTTPException(status_code=500, detail=f"Pinecone initialization failed: {e}")
    
    return _pinecone_service

def get_rag_service() -> RAGService:
    """Get or initialize RAG service"""
    global _rag_service
    if _rag_service is None:
        try:
            openai_service = get_openai_service()
            pinecone_service = get_pinecone_service()
            _rag_service = RAGService(
                openai_service=openai_service,
                pinecone_service=pinecone_service
            )
            logger.info("RAG service initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing RAG service: {e}")
            raise HTTPException(status_code=500, detail=f"RAG initialization failed: {e}")
    
    return _rag_service

# Core functions from simple_app.py
def get_workflows(client):
    """Get available workflows"""
    try:
        response = client.workflows.list_workflows(
            request=ListWorkflowsRequest()
        )
        workflows = response.response_list_workflows if response.response_list_workflows else []
        logger.info(f"Found {len(workflows)} workflows")
        return workflows
    except Exception as e:
        logger.error(f"Error fetching workflows: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching workflows: {e}")

def get_active_workflow(client):
    """Get the first active workflow"""
    workflows = get_workflows(client)
    active_workflows = [w for w in workflows if w.status.lower() == 'active']
    if not active_workflows:
        raise HTTPException(status_code=404, detail="No active workflows found")
    return active_workflows[0]

def upload_file_to_gcs(gcs_client, bucket_name: str, file_content: bytes, filename: str, custom_folder: Optional[str] = None):
    """Upload file to Google Cloud Storage"""
    try:
        logger.info(f"Starting upload: {filename} to bucket {bucket_name}")
        
        bucket = gcs_client.bucket(bucket_name)
        
        # Use custom folder or default path
        upload_path = f"{custom_folder}/" if custom_folder else "protocols/dev/"
        blob_name = f"{upload_path}{filename}"
        blob = bucket.blob(blob_name)
        
        # Upload the file content
        blob.upload_from_string(file_content, content_type='application/pdf')
        logger.info(f"Upload completed: {blob_name}")
        
        # Get file metadata
        blob.reload()
        
        result = {
            'success': True,
            'blob_name': blob_name,
            'bucket': bucket_name,
            'public_url': f"gs://{bucket_name}/{blob_name}",
            'size': blob.size,
            'created': blob.time_created.isoformat() if blob.time_created else None,
            'updated': blob.updated.isoformat() if blob.updated else None
        }
        
        logger.info(f"Upload result: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Error uploading to GCS: {e}")
        raise HTTPException(status_code=500, detail=f"GCS upload failed: {e}")

def trigger_workflow(unstructured_client, workflow_id: str, namespace: Optional[str] = None, pinecone_connector_id: Optional[str] = None, retry_count: int = 0, max_retries: int = 3):
    """Manually trigger a workflow to process uploaded files with optional namespace update and rate limiting protection"""
    try:
        # Rate limiting protection with exponential backoff
        if retry_count > 0:
            wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30 seconds
            logger.info(f"Retry {retry_count}/{max_retries}, waiting {wait_time}s before retry...")
            time.sleep(wait_time)
        
        # If namespace is provided, update the Pinecone connector first
        if namespace and pinecone_connector_id:
            logger.info(f"🔄 Updating Pinecone namespace to: `{namespace}`")
            logger.info(f"   • Connector ID: {pinecone_connector_id}")
            logger.info(f"   • Target Namespace: {namespace}")
            namespace_result = update_pinecone_namespace(
                unstructured_client, 
                pinecone_connector_id, 
                namespace
            )
            
            if not namespace_result.get('success'):
                logger.warning("⚠️ Namespace update failed, proceeding with current namespace")
                logger.warning(f"   • Error: {namespace_result.get('error', 'Unknown error')}")
            else:
                logger.info(f"✅ Namespace updated to: `{namespace}`")
        elif namespace and not pinecone_connector_id:
            logger.warning(f"⚠️ Namespace '{namespace}' provided but no Pinecone connector found")
        elif not namespace:
            logger.info("ℹ️ No namespace provided, using default Pinecone namespace")
        
        logger.info(f"Triggering workflow {workflow_id}")
        
        # Print workflow input details
        print(f"\n=== WORKFLOW EXECUTION STARTED ===")
        print(f"📥 Workflow Input Details:")
        print(f"   • Workflow ID: {workflow_id}")
        print(f"   • Namespace: {namespace or 'default'}")
        print(f"   • Pinecone Connector ID: {pinecone_connector_id or 'N/A'}")
        print(f"   • Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Create the workflow run request (only workflow_id is supported)
        response = unstructured_client.workflows.run_workflow(
            request=RunWorkflowRequest(
                workflow_id=workflow_id
            )
        )
        
        if response and hasattr(response, 'raw_response'):
            logger.info(f"Workflow triggered successfully: {response.raw_response}")
            
            # Try to extract job information from response
            job_info = None
            if hasattr(response, 'job_information'):
                job_info = response.job_information
            elif hasattr(response, 'response_run_workflow'):
                job_info = response.response_run_workflow
            
            if job_info:
                job_id = getattr(job_info, 'id', 'Unknown')
                job_status = getattr(job_info, 'status', 'Unknown')
                
                # Print workflow output details
                print(f"\n📤 Workflow Output Details:")
                print(f"   • Job ID: {job_id}")
                print(f"   • Initial Status: {job_status}")
                print(f"   • Response Type: {type(response).__name__}")
                print(f"   • Job Info: {job_info}")
                print(f"   • Success: True")
                print(f"=== WORKFLOW EXECUTION COMPLETED ===\n")
                
                logger.info(f"✅ Workflow triggered successfully!")
                logger.info(f"📋 Job ID: {job_id}")
                logger.info(f"🔄 Initial Status: {job_status}")
                if namespace:
                    logger.info(f"🏷️ Using namespace: `{namespace}`")
                return {'success': True, 'job_id': job_id, 'job_info': job_info, 'namespace': namespace}
            else:
                # Print workflow output details for case without job info
                print(f"\n📤 Workflow Output Details:")
                print(f"   • Response Type: {type(response).__name__}")
                print(f"   • Raw Response: {getattr(response, 'raw_response', 'N/A')}")
                print(f"   • Success: True")
                print(f"   • Note: Job details available in monitoring")
                print(f"=== WORKFLOW EXECUTION COMPLETED ===\n")
                
                logger.info(f"✅ Workflow triggered successfully!")
                logger.info("📋 Job details will be available in workflow monitoring")
                if namespace:
                    logger.info(f"🏷️ Using namespace: `{namespace}`")
                return {'success': True, 'response': response, 'namespace': namespace}
        else:
            # Print workflow output for basic success case
            print(f"\n📤 Workflow Output Details:")
            print(f"   • Response: Basic success (no detailed response object)")
            print(f"   • Success: True")
            print(f"=== WORKFLOW EXECUTION COMPLETED ===\n")
            
            logger.info(f"✅ Workflow trigger request sent")
            return {'success': True, 'namespace': namespace}
            
    except Exception as e:
        # Check if it's a rate limit error (429)
        error_str = str(e)
        if "429" in error_str or "Rate limit" in error_str:
            if retry_count < max_retries:
                logger.warning(f"Rate limit hit, retrying... ({retry_count + 1}/{max_retries})")
                return trigger_workflow(
                    unstructured_client, 
                    workflow_id, 
                    namespace, 
                    pinecone_connector_id, 
                    retry_count + 1,
                    max_retries
                )
            else:
                logger.error(f"Max retries reached for rate limiting")
                return {'success': False, 'error': f"Rate limit exceeded after {max_retries} retries"}
        
        # Print workflow error details
        print(f"\n❌ WORKFLOW EXECUTION FAILED")
        print(f"📤 Error Details:")
        print(f"   • Error Type: {type(e).__name__}")
        print(f"   • Error Message: {str(e)}")
        print(f"   • Success: False")
        print(f"=== WORKFLOW EXECUTION COMPLETED ===\n")
        
        logger.error(f"Error triggering workflow: {e}")
        return {'success': False, 'error': str(e)}

def add_job_to_queue(job_data: Dict[str, Any]):
    """Add a job to Pub/Sub queue"""
    try:
        publisher = get_pubsub_publisher()
        topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, PUBSUB_TOPIC)
        
        # Serialize job data to JSON bytes
        message_data = json.dumps(job_data).encode('utf-8')
        
        # Publish message
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()  # Wait for publish to complete
        
        global job_queue_count
        job_queue_count += 1
        logger.info(f"Job added to queue: {job_data['filename']} (Message ID: {message_id})")
        return True
        
    except Exception as e:
        logger.error(f"Failed to add job to queue: {e}")
        return False

def get_destination_connectors(client):
    """Get available destination connectors"""
    try:
        response = client.destinations.list_destinations(
            request=ListDestinationsRequest()
        )
        destinations = response.response_list_destinations if response.response_list_destinations else []
        logger.info(f"Found {len(destinations)} destination connectors")
        return destinations
    except Exception as e:
        logger.error(f"Error fetching destination connectors: {e}")
        return []

def get_pinecone_connectors(client):
    """Get Pinecone destination connectors specifically"""
    destinations = get_destination_connectors(client)
    pinecone_connectors = [d for d in destinations if getattr(d, 'type', '').lower() == 'pinecone']
    logger.info(f"Found {len(pinecone_connectors)} Pinecone connectors")
    return pinecone_connectors

def update_pinecone_namespace(client, connector_id, new_namespace, connector_config=None):
    """Update the namespace for a Pinecone destination connector"""
    try:
        logger.info(f"Updating Pinecone connector {connector_id} namespace to: {new_namespace}")
        
        # Get current connector details if config not provided
        if not connector_config:
            destinations = get_destination_connectors(client)
            connector = next((d for d in destinations if d.id == connector_id), None)
            if not connector:
                raise Exception(f"Connector {connector_id} not found")
            connector_config = getattr(connector, 'config', None)
        
        # Convert Pydantic model to dict and update with new namespace
        if hasattr(connector_config, 'model_dump'):
            # Handle Pydantic v2 model
            updated_config = connector_config.model_dump()
        elif hasattr(connector_config, 'dict'):
            # Handle Pydantic v1 model
            updated_config = connector_config.dict()
        elif hasattr(connector_config, '__dict__'):
            # Handle other object types
            updated_config = {}
            for key, value in connector_config.__dict__.items():
                if not key.startswith('_') and not callable(value):
                    updated_config[key] = value
        else:
            # Handle dict-like object
            updated_config = dict(connector_config) if connector_config else {}
        
        # Ensure all required Pinecone configuration fields are present
        # Get required values from environment or preserve existing
        pinecone_index = os.getenv("PINECONE_INDEX_NAME")
        pinecone_api_key = os.getenv("PINECONE_API_KEY")
        
        # Validate required fields
        if not pinecone_index:
            raise Exception("PINECONE_INDEX_NAME environment variable is required")
        if not pinecone_api_key:
            raise Exception("PINECONE_API_KEY environment variable is required")
        
        # Build complete configuration with all required fields
        complete_config = {
            "index_name": pinecone_index,
            "namespace": new_namespace,
            "api_key": pinecone_api_key,
            "batch_size": updated_config.get("batch_size", 50)  # Default to 50 if not set
        }
        
        # Preserve any additional configuration fields that might exist
        for key, value in updated_config.items():
            if key not in complete_config and value is not None:
                complete_config[key] = value
        
        logger.info(f"Sending Pinecone config update: {complete_config}")
        logger.info(f"Config keys: {list(complete_config.keys())}")
        
        # Create update request with corrected structure
        response = client.destinations.update_destination(
            request=UpdateDestinationRequest(
                destination_id=connector_id,  # ✅ FIXED: destination_id inside the request
                update_destination_connector=UpdateDestinationConnector(
                    config=complete_config
                )
            )
        )
        
        if response:
            logger.info(f"Successfully updated namespace to: {new_namespace}")
            return {'success': True, 'namespace': new_namespace}
        else:
            raise Exception("Update request returned no response")
            
    except Exception as e:
        logger.error(f"Error updating Pinecone namespace: {e}")
        return {'success': False, 'error': str(e)}

def check_job_status(client, job_id):
    """Check the status of a workflow job"""
    try:
        response = client.jobs.get_job(job_id)
        return response.job_information if response else None
    except Exception as e:
        logger.error(f"Error checking job status: {e}")
        return None

def check_workflow_jobs(client, workflow_id: str, limit: int = 10):
    """Check recent jobs for a workflow - using exact pattern from run_workflow_test.py"""
    try:
        # Use the exact same pattern as the working run_workflow_test.py
        jobs_response = client.jobs.list_jobs(
            request=ListJobsRequest(workflow_id=workflow_id)
        )
        
        if jobs_response.response_list_jobs:
            # Sort by creation time (most recent first) - same as working code
            jobs = sorted(jobs_response.response_list_jobs,
                         key=lambda j: j.created_at or "", reverse=True)
            logger.info(f"Found {len(jobs)} jobs for workflow {workflow_id}")
            return jobs[:limit]
        else:
            logger.info(f"No jobs found for workflow {workflow_id}")
            return []
    except Exception as e:
        logger.error(f"Error checking jobs: {e}")
        return []

# Twilio AI Calling Functions (now using services)

async def query_pinecone_context(query_text: str, namespace: str, top_k: int = 5) -> str:
    """
    Query Pinecone for relevant context using RAG.
    This is a wrapper around RAGService for backward compatibility.
    
    Args:
        query_text: The text to search for
        namespace: Pinecone namespace to search in
        top_k: Number of results to retrieve
        
    Returns:
        Formatted context string from top results
    """
    try:
        rag_service = get_rag_service()
        return await rag_service.query_context(
            query_text=query_text,
            namespace=namespace,
            top_k=top_k
        )
    except Exception as e:
        logger.error(f"Error in query_pinecone_context: {e}")
        return f"Error retrieving context: {str(e)}"

# API Routes
@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "PDF Upload & Workflow API", "status": "active"}

@app.get("/test", response_class=HTMLResponse)
async def test_page():
    """Serve the test conversation page"""
    html_file = Path(__file__).parent / "static" / "test_conversation.html"
    if html_file.exists():
        return HTMLResponse(content=html_file.read_text(), status_code=200)
    else:
        return HTMLResponse(
            content="<h1>Test page not found</h1><p>Please ensure static/test_conversation.html exists.</p>",
            status_code=404
        )

@app.get("/workflows", response_model=List[WorkflowInfo])
async def list_workflows():
    """List all available workflows"""
    client = get_unstructured_client()
    workflows = get_workflows(client)
    
    return [
        WorkflowInfo(
            id=w.id,
            name=w.name,
            status=w.status,
            created_at=getattr(w, 'created_at', None).isoformat() if getattr(w, 'created_at', None) else None,
            updated_at=getattr(w, 'updated_at', None).isoformat() if getattr(w, 'updated_at', None) else None
        )
        for w in workflows
    ]

@app.get("/workflows/active", response_model=WorkflowInfo)
async def get_active_workflow_info():
    """Get the first active workflow"""
    client = get_unstructured_client()
    workflow = get_active_workflow(client)
    
    return WorkflowInfo(
        id=workflow.id,
        name=workflow.name,
        status=workflow.status,
        created_at=getattr(workflow, 'created_at', None).isoformat() if getattr(workflow, 'created_at', None) else None,
        updated_at=getattr(workflow, 'updated_at', None).isoformat() if getattr(workflow, 'updated_at', None) else None
    )

@app.get("/connectors/pinecone")
async def list_pinecone_connectors():
    """List Pinecone destination connectors"""
    client = get_unstructured_client()
    connectors = get_pinecone_connectors(client)
    
    return [
        {
            "id": getattr(c, 'id', 'Unknown'),
            "name": getattr(c, 'name', 'Unnamed'),
            "type": getattr(c, 'type', 'Unknown'),
            "config": {
                "namespace": getattr(getattr(c, 'config', None), 'namespace', 'default') if getattr(c, 'config', None) else 'default',
                "index_name": getattr(getattr(c, 'config', None), 'index_name', 'Unknown') if getattr(c, 'config', None) else 'Unknown'
            }
        }
        for c in connectors
    ]

@app.post("/upload", response_model=UploadResponse)
async def upload_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    namespace: Optional[str] = Form(None),
    workflow_id: Optional[str] = Form(None),
    custom_folder: Optional[str] = Form(None)
):
    """Upload a PDF file and trigger workflow processing"""
    
    # Log user request data
    logger.info(f"\n=== UPLOAD REQUEST RECEIVED ===")
    logger.info(f"📥 User Request Details:")
    logger.info(f"   • Filename: {file.filename}")
    logger.info(f"   • File Size: {file.size if hasattr(file, 'size') else 'Unknown'} bytes")
    logger.info(f"   • Content Type: {file.content_type}")
    logger.info(f"   • Namespace: {namespace}")
    logger.info(f"   • Workflow ID: {workflow_id}")
    logger.info(f"   • Custom Folder: {custom_folder}")
    logger.info(f"   • Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"=== END REQUEST DETAILS ===\n")
    
    # Validate file type
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")
    
    try:
        # Get clients
        client = get_unstructured_client()
        gcs_client = get_gcs_client()
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        
        if not bucket_name:
            raise HTTPException(status_code=500, detail="GCS_BUCKET_NAME not configured")
        
        # Get active workflow if not specified
        if not workflow_id:
            active_workflow = get_active_workflow(client)
            workflow_id = active_workflow.id
            workflow_name = active_workflow.name
        else:
            workflows = get_workflows(client)
            workflow = next((w for w in workflows if w.id == workflow_id), None)
            if not workflow:
                raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
            workflow_name = workflow.name
        
        # Read file content
        file_content = await file.read()
        
        # Upload to GCS
        upload_result = upload_file_to_gcs(
            gcs_client, 
            bucket_name, 
            file_content, 
            file.filename,
            custom_folder
        )
        
        if not upload_result.get('success'):
            raise HTTPException(status_code=500, detail="Failed to upload to GCS")
        
        # Prepare job data
        job_data = {
            'filename': file.filename,
            'gcs_path': upload_result['blob_name'],
            'bucket': upload_result['bucket'],
            'workflow_id': workflow_id,
            'workflow_name': workflow_name,
            'namespace': namespace,
            'uploaded_at': time.time()
        }
        
        # Always trigger workflow directly after upload
        logger.info(f"Triggering workflow directly for {file.filename}")
        
        # Get Pinecone connector ID if namespace is provided
        pinecone_connector_id = None
        if namespace:  # Changed: Remove the "!= default" check
            try:
                pinecone_connectors = get_pinecone_connectors(client)
                if pinecone_connectors:
                    pinecone_connector_id = pinecone_connectors[0].id
                    logger.info(f"Using Pinecone connector: {pinecone_connector_id}")
                    logger.info(f"Will update namespace to: {namespace}")
                else:
                    logger.warning("No Pinecone connectors found")
            except Exception as e:
                logger.warning(f"Could not get Pinecone connector: {e}")
        else:
            logger.info("No namespace provided, using default Pinecone namespace")
        
        # Trigger workflow
        workflow_result = trigger_workflow(
            client, 
            workflow_id, 
            namespace=namespace,  # Changed: Always pass namespace if provided
            pinecone_connector_id=pinecone_connector_id
        )
        
        job_id = None
        if workflow_result.get('success') and 'job_id' in workflow_result:
            job_id = workflow_result['job_id']
            logger.info(f"Workflow triggered successfully with job ID: {job_id}")
        elif workflow_result.get('success'):
            logger.info("Workflow triggered successfully (no job ID returned)")
        else:
            logger.error(f"Workflow trigger failed: {workflow_result.get('error', 'Unknown error')}")
        
        # Optionally add to queue for monitoring (this is now secondary)
        try:
            add_job_to_queue(job_data)
            logger.info(f"Job also added to monitoring queue for {file.filename}")
        except Exception as e:
            logger.warning(f"Could not add job to queue (not critical): {e}")
        
        # Log final response details
        logger.info(f"\n=== UPLOAD RESPONSE ===")
        logger.info(f"📤 Response Details:")
        logger.info(f"   • Success: True")
        logger.info(f"   • Filename: {file.filename}")
        logger.info(f"   • GCS Path: {upload_result['blob_name']}")
        logger.info(f"   • Bucket: {upload_result['bucket']}")
        logger.info(f"   • Size: {upload_result['size']} bytes")
        logger.info(f"   • Job ID: {job_id}")
        logger.info(f"   • Workflow ID: {workflow_id}")
        logger.info(f"   • Namespace Used: {namespace}")
        logger.info(f"=== END RESPONSE ===\n")
        
        return UploadResponse(
            success=True,
            filename=file.filename,
            gcs_path=upload_result['blob_name'],
            bucket=upload_result['bucket'],
            size=upload_result['size'],
            job_id=job_id,
            workflow_id=workflow_id,
            namespace=namespace,
            message="File uploaded successfully and workflow triggered"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/workflows/{workflow_id}/trigger")
async def trigger_workflow_manually(
    workflow_id: str,
    namespace: Optional[str] = None
):
    """Manually trigger a specific workflow"""
    client = get_unstructured_client()
    
    # Verify workflow exists
    workflows = get_workflows(client)
    workflow = next((w for w in workflows if w.id == workflow_id), None)
    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")
    
    # Get Pinecone connector ID if namespace is provided
    pinecone_connector_id = None
    if namespace:  # Changed: Remove the "!= default" check
        try:
            pinecone_connectors = get_pinecone_connectors(client)
            if pinecone_connectors:
                pinecone_connector_id = pinecone_connectors[0].id
                logger.info(f"Using Pinecone connector: {pinecone_connector_id}")
                logger.info(f"Will update namespace to: {namespace}")
        except Exception as e:
            logger.warning(f"Could not get Pinecone connector: {e}")
    else:
        logger.info("No namespace provided, using default Pinecone namespace")
    
    result = trigger_workflow(
        client, 
        workflow_id, 
        namespace=namespace,  # Changed: Always pass namespace if provided
        pinecone_connector_id=pinecone_connector_id
    )
    
    if result.get('success'):
        return {
            "success": True,
            "workflow_id": workflow_id,
            "job_id": result.get('job_id'),
            "namespace": namespace,
            "message": "Workflow triggered successfully"
        }
    else:
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to trigger workflow: {result.get('error', 'Unknown error')}"
        )

@app.get("/workflows/{workflow_id}/jobs", response_model=List[JobStatus])
async def get_workflow_jobs(workflow_id: str, limit: int = 10):
    """Get recent jobs for a specific workflow"""
    client = get_unstructured_client()
    jobs = check_workflow_jobs(client, workflow_id, limit)
    
    return [
        JobStatus(
            job_id=getattr(job, 'id', 'Unknown'),
            status=getattr(job, 'status', 'Unknown'),
            created_at=getattr(job, 'created_at', None).isoformat() if getattr(job, 'created_at', None) else None,
            updated_at=getattr(job, 'updated_at', None).isoformat() if getattr(job, 'updated_at', None) else None,
            workflow_name=getattr(job, 'workflow_name', None)
        )
        for job in jobs
    ]

@app.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Get status of a specific job"""
    client = get_unstructured_client()
    
    try:
        response = client.jobs.get_job(job_id)
        job_info = response.job_information if response else None
        
        if job_info:
            return JobStatus(
                job_id=getattr(job_info, 'id', job_id),
                status=getattr(job_info, 'status', 'Unknown'),
                created_at=getattr(job_info, 'created_at', None).isoformat() if getattr(job_info, 'created_at', None) else None,
                updated_at=getattr(job_info, 'updated_at', None).isoformat() if getattr(job_info, 'updated_at', None) else None,
                workflow_name=getattr(job_info, 'workflow_name', None)
            )
        else:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
            
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting job status: {e}")

@app.get("/status")
async def get_system_status():
    """Get overall system status"""
    global current_job_id, job_queue_count
    
    with current_job_lock:
        active_job_id = current_job_id
        queue_count = job_queue_count
    
    return {
        "system_status": "operational",
        "current_job": active_job_id,
        "jobs_in_queue": queue_count,
        "timestamp": datetime.now().isoformat()
    }

# Twilio AI Calling Endpoints

@app.post("/call/interactive", response_model=CallResponse)
async def initiate_interactive_call(
    call_request: CallRequest,
    background_tasks: BackgroundTasks,
    request: Request
):
    """
    Initiate an interactive AI call with RAG support
    
    Args:
        call_request: Call parameters including phone_number, namespace, initial_message
        background_tasks: FastAPI background tasks
        request: Request object to get base URL
        
    Returns:
        CallResponse with call details
    """
    try:
        logger.info(f"\n=== INTERACTIVE CALL REQUEST ===")
        logger.info(f"Phone Number: {call_request.phone_number}")
        logger.info(f"Namespace: {call_request.namespace}")
        logger.info(f"Context Text Length: {len(call_request.context_text) if call_request.context_text else 0}")
        logger.info(f"Initial Message: {call_request.initial_message}")
        
        # Validate that either namespace or context_text is provided
        if not call_request.namespace and not call_request.context_text:
            raise HTTPException(
                status_code=400,
                detail="Either 'namespace' or 'context_text' must be provided"
            )
        
        # Validate phone number format
        phone = call_request.phone_number.strip()
        twilio_service = get_twilio_service()
        
        if not twilio_service.validate_phone_number(phone):
            raise HTTPException(status_code=400, detail="Phone number must be in E.164 format (start with +)")
        
        # Build WebSocket URL (use request base URL for production)
        base_url = str(request.base_url).rstrip('/')
        ws_url = base_url.replace('http://', 'ws://').replace('https://', 'wss://')
        
        # Add parameters to WebSocket URL
        ws_params = []
        if call_request.namespace:
            ws_params.append(f"namespace={call_request.namespace}")
        if call_request.context_text:
            # Store context_text in a way it can be retrieved (we'll use a simple in-memory store)
            import hashlib
            context_id = hashlib.md5(call_request.context_text.encode()).hexdigest()
            # Store in global dict (you might want to use Redis in production)
            if 'context_store' not in globals():
                globals()['context_store'] = {}
            globals()['context_store'][context_id] = call_request.context_text
            ws_params.append(f"context_id={context_id}")
        
        websocket_url = f"{ws_url}/ws/twilio-stream?{'&'.join(ws_params)}"
        
        logger.info(f"WebSocket URL: {websocket_url}")
        
        # Generate TwiML using service
        twiml = twilio_service.generate_interactive_twiml(
            websocket_url, 
            call_request.namespace or "direct-context"
        )
        
        # Initiate call using service
        call_result = twilio_service.initiate_call(
            to_number=phone,
            twiml=twiml,
            status_callback_url=f"{base_url}/webhooks/twilio/call-status"
        )
        
        logger.info(f"Call initiated - SID: {call_result['call_sid']}, Status: {call_result['status']}")
        logger.info(f"=== END CALL REQUEST ===\n")
        
        return CallResponse(
            success=True,
            call_sid=call_result['call_sid'],
            phone_number=phone,
            status=call_result['status'],
            message="Interactive call initiated successfully",
            namespace=call_request.namespace
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating interactive call: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to initiate call: {str(e)}")

@app.post("/call/alert", response_model=CallResponse)
async def initiate_alert_call(
    alert_request: AlertCallRequest,
    background_tasks: BackgroundTasks,
    request: Request
):
    """
    Initiate a simple alert call that plays a message and hangs up
    
    Args:
        alert_request: Alert parameters including phone_number and message
        background_tasks: FastAPI background tasks
        request: Request object to get base URL
        
    Returns:
        CallResponse with call details
    """
    try:
        logger.info(f"\n=== ALERT CALL REQUEST ===")
        logger.info(f"Phone Number: {alert_request.phone_number}")
        logger.info(f"Message: {alert_request.message}")
        
        # Validate phone number format
        phone = alert_request.phone_number.strip()
        twilio_service = get_twilio_service()
        
        if not twilio_service.validate_phone_number(phone):
            raise HTTPException(status_code=400, detail="Phone number must be in E.164 format (start with +)")
        
        # Generate TwiML for alert using service
        twiml = twilio_service.generate_alert_twiml(alert_request.message)
        
        # Build callback URL
        base_url = str(request.base_url).rstrip('/')
        
        # Initiate call using service
        call_result = twilio_service.initiate_call(
            to_number=phone,
            twiml=twiml,
            status_callback_url=f"{base_url}/webhooks/twilio/call-status"
        )
        
        logger.info(f"Alert call initiated - SID: {call_result['call_sid']}, Status: {call_result['status']}")
        logger.info(f"=== END ALERT CALL REQUEST ===\n")
        
        return CallResponse(
            success=True,
            call_sid=call_result['call_sid'],
            phone_number=phone,
            status=call_result['status'],
            message="Alert call initiated successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error initiating alert call: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to initiate alert call: {str(e)}")

@app.get("/call/status/{call_sid}")
async def get_call_status_endpoint(call_sid: str):
    """
    Get the status of a specific call
    
    Args:
        call_sid: Twilio call SID
        
    Returns:
        Call status information
    """
    try:
        twilio_service = get_twilio_service()
        status = twilio_service.get_call_status(call_sid)
        return status
        
    except Exception as e:
        logger.error(f"Error fetching call status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch call status: {str(e)}")

@app.post("/webhooks/twilio/call-status")
async def twilio_call_status_callback(request: Request):
    """
    Webhook to receive call status updates from Twilio
    
    Args:
        request: Request containing Twilio callback data
        
    Returns:
        Success acknowledgment
    """
    try:
        form_data = await request.form()
        call_sid = form_data.get('CallSid')
        call_status = form_data.get('CallStatus')
        from_number = form_data.get('From')
        to_number = form_data.get('To')
        duration = form_data.get('CallDuration')
        
        logger.info(f"\n=== TWILIO CALL STATUS UPDATE ===")
        logger.info(f"Call SID: {call_sid}")
        logger.info(f"Status: {call_status}")
        logger.info(f"From: {from_number}")
        logger.info(f"To: {to_number}")
        logger.info(f"Duration: {duration} seconds")
        logger.info(f"=== END STATUS UPDATE ===\n")
        
        # Optional: Publish to Pub/Sub or store in database
        try:
            status_data = {
                'call_sid': call_sid,
                'status': call_status,
                'from': from_number,
                'to': to_number,
                'duration': duration,
                'timestamp': datetime.now().isoformat()
            }
            # Could publish to Pub/Sub here if needed
            # add_job_to_queue(status_data)
        except Exception as e:
            logger.warning(f"Could not publish status update: {e}")
        
        return {"success": True}
        
    except Exception as e:
        logger.error(f"Error processing Twilio callback: {e}")
        return {"success": False, "error": str(e)}

# WebSocket endpoint for Twilio Media Streams with OpenAI Real-time API

@app.websocket("/ws/twilio-stream")
async def twilio_media_stream(websocket: WebSocket):
    """
    WebSocket endpoint for Twilio Media Streams
    Handles bidirectional audio streaming with OpenAI Real-time API and Pinecone RAG
    """
    await websocket.accept()
    
    logger.info("Twilio WebSocket connection accepted")
    
    # Extract parameters from query parameters
    namespace = websocket.query_params.get('namespace')
    context_id = websocket.query_params.get('context_id')
    
    # Retrieve context_text if context_id is provided
    direct_context = None
    if context_id and 'context_store' in globals():
        direct_context = globals()['context_store'].get(context_id)
        logger.info(f"Using direct context (length: {len(direct_context) if direct_context else 0})")
    
    if namespace:
        logger.info(f"Using Pinecone namespace: {namespace}")
    
    # Connection state
    openai_ws = None
    stream_sid = None
    call_sid = None
    
    try:
        # Get services
        openai_service = get_openai_service()
        rag_service = get_rag_service() if namespace else None
        
        # Build instructions based on mode
        if direct_context:
            instructions = f"""You are a helpful health-focused AI assistant. 

IMPORTANT RESTRICTIONS:
1. ONLY answer questions related to health, medical, wellness, healthcare, clinical, or medical topics.
2. If a question is NOT related to health/medical topics, politely decline and say: "I can only answer health-related questions. Please ask me about health, medical, or wellness topics."
3. Use the following context to answer questions accurately:

CONTEXT:
{direct_context}

If the answer is not in the provided context, say "I don't have that specific information in my current context."
"""
        else:
            instructions = """You are a helpful health-focused AI assistant with access to a medical knowledge base.

IMPORTANT RESTRICTIONS:
1. ONLY answer questions related to health, medical, wellness, healthcare, clinical, or medical topics.
2. If a question is NOT related to health/medical topics, politely decline and say: "I can only answer health-related questions. Please ask me about health, medical, or wellness topics."
3. Use the provided context from the knowledge base to answer questions accurately.
4. If you don't find relevant information in the context, say so honestly.
"""
        
        # Configure and connect to OpenAI Real-time API using service
        # Note: Twilio uses g711_ulaw format which is native for phone calls
        session_config = {
            "type": "session.update",
            "session": {
                "modalities": ["text", "audio"],
                "instructions": instructions,
                "voice": "alloy",
                "input_audio_format": "g711_ulaw",  # Twilio's native format
                "output_audio_format": "g711_ulaw", # Twilio's native format
                "input_audio_transcription": {
                    "model": "whisper-1"
                },
                "turn_detection": {
                    "type": "server_vad",
                    "threshold": 0.5,
                    "prefix_padding_ms": 300,
                    "silence_duration_ms": 500
                },
                "temperature": 0.7,
                "max_response_output_tokens": 4096
            }
        }
        
        openai_ws = await openai_service.connect_realtime_api(session_config)
        logger.info("OpenAI session configured")
        
        # Task for receiving from Twilio and sending to OpenAI
        async def twilio_to_openai():
            nonlocal stream_sid, call_sid
            try:
                async for message in websocket.iter_text():
                    data = json.loads(message)
                    event_type = data.get('event')
                    
                    if event_type == 'start':
                        stream_sid = data['start']['streamSid']
                        call_sid = data['start']['callSid']
                        logger.info(f"Stream started - StreamSID: {stream_sid}, CallSID: {call_sid}")
                        
                    elif event_type == 'media':
                        # Forward audio to OpenAI using service
                        media_payload = data['media']['payload']
                        await openai_service.send_audio_to_realtime(openai_ws, media_payload)
                        
                    elif event_type == 'stop':
                        logger.info(f"Stream stopped - StreamSID: {stream_sid}")
                        break
                        
            except WebSocketDisconnect:
                logger.info("Twilio WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error in twilio_to_openai: {e}")
        
        # Task for receiving from OpenAI and sending to Twilio
        async def openai_to_twilio():
            try:
                last_transcript = ""
                
                async for message in openai_ws:
                    data = json.loads(message)
                    event_type = data.get('type')
                    
                    # Handle audio responses from OpenAI
                    if event_type == 'response.audio.delta':
                        audio_delta = data.get('delta')
                        if audio_delta:
                            # Send audio to Twilio
                            media_message = {
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {
                                    "payload": audio_delta
                                }
                            }
                            await websocket.send_json(media_message)
                    
                    # Handle transcripts for RAG queries
                    elif event_type == 'conversation.item.input_audio_transcription.completed':
                        transcript = data.get('transcript', '')
                        if transcript and transcript != last_transcript:
                            last_transcript = transcript
                            logger.info(f"User said: {transcript}")
                            
                            # Only query Pinecone if namespace is provided (not using direct context)
                            if namespace and rag_service:
                                try:
                                    context = await rag_service.query_context(
                                        query_text=transcript,
                                        namespace=namespace,
                                        top_k=5
                                    )
                                    logger.info(f"Retrieved context from Pinecone (length: {len(context)})")
                                    
                                    # Inject context into conversation using service
                                    if context and "No relevant context" not in context:
                                        await openai_service.inject_context_to_conversation(openai_ws, context)
                                        
                                except Exception as e:
                                    logger.error(f"Error querying Pinecone: {e}")
                            elif direct_context:
                                # Using direct context - it's already in the system instructions
                                logger.info(f"Using direct context (already provided in instructions)")
                    
                    # Handle response completion
                    elif event_type == 'response.done':
                        logger.info("Response completed")
                    
                    # Handle errors
                    elif event_type == 'error':
                        error_info = data.get('error', {})
                        logger.error(f"OpenAI error: {error_info}")
                        
            except Exception as e:
                logger.error(f"Error in openai_to_twilio: {e}")
        
        # Run both tasks concurrently
        await asyncio.gather(
            twilio_to_openai(),
            openai_to_twilio()
        )
        
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Clean up
        if openai_ws:
            await openai_ws.close()
            logger.info("OpenAI WebSocket closed")
        
        try:
            await websocket.close()
            logger.info("Twilio WebSocket closed")
        except:
            pass
        
        logger.info(f"WebSocket session ended - CallSID: {call_sid}")

@app.websocket("/ws/test-conversation")
async def test_conversation(websocket: WebSocket):
    """
    WebSocket endpoint for testing conversational AI without making actual phone calls.
    This simulates the same flow as Twilio but uses browser audio instead.
    """
    await websocket.accept()
    
    logger.info("Test conversation WebSocket connection accepted")
    
    # Extract parameters from query
    namespace = websocket.query_params.get('namespace', 'test')
    voice = websocket.query_params.get('voice', 'alloy')
    logger.info(f"Test conversation - Namespace: {namespace}, Voice: {voice}")
    
    # Connection state
    openai_ws = None
    
    try:
        # Get services
        openai_service = get_openai_service()
        rag_service = get_rag_service()
        
        # Send initial status
        await websocket.send_json({
            'type': 'status',
            'message': f'Connecting to OpenAI Realtime API with voice: {voice}'
        })
        
        # Configure and connect to OpenAI Real-time API
        session_config = openai_service.get_default_session_config(
            instructions=f"You are a helpful AI assistant. Use the provided context from the knowledge base (namespace: {namespace}) to answer questions accurately. If you don't find relevant information in the context, say so honestly.",
            voice=voice,
            temperature=0.7
        )
        
        openai_ws = await openai_service.connect_realtime_api(session_config)
        logger.info("Test conversation: OpenAI session configured")
        
        await websocket.send_json({
            'type': 'status',
            'message': '✓ Connected to OpenAI Realtime API'
        })
        
        # Task for receiving from browser and sending to OpenAI
        async def browser_to_openai():
            try:
                logger.info("Test conversation: browser_to_openai task started")
                async for message in websocket.iter_text():
                    data = json.loads(message)
                    message_type = data.get('type')
                    
                    if message_type == 'audio':
                        # Forward audio to OpenAI
                        audio_data = data.get('audio', '')
                        if audio_data:
                            await openai_service.send_audio_to_realtime(openai_ws, audio_data)
                    
                    elif message_type == 'stop':
                        logger.info("Test conversation: Stop requested")
                        break
                        
            except WebSocketDisconnect:
                logger.info("Test conversation: Browser WebSocket disconnected")
            except Exception as e:
                logger.error(f"Error in browser_to_openai: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        # Task for receiving from OpenAI and sending to browser
        async def openai_to_browser():
            try:
                logger.info("Test conversation: openai_to_browser task started")
                last_transcript = ""
                
                async for message in openai_ws:
                    data = json.loads(message)
                    event_type = data.get('type')
                    
                    # Handle audio responses from OpenAI
                    if event_type == 'response.audio.delta':
                        audio_delta = data.get('delta')
                        if audio_delta:
                            # Send audio to browser
                            await websocket.send_json({
                                'type': 'audio',
                                'audio_delta': audio_delta
                            })
                    
                    # Handle transcripts for RAG queries
                    elif event_type == 'conversation.item.input_audio_transcription.completed':
                        transcript = data.get('transcript', '')
                        if transcript and transcript != last_transcript:
                            last_transcript = transcript
                            logger.info(f"Test conversation - User said: {transcript}")
                            
                            # Send transcript to browser
                            await websocket.send_json({
                                'type': 'transcript',
                                'text': transcript
                            })
                            
                            # Query Pinecone for context
                            try:
                                await websocket.send_json({
                                    'type': 'rag_query',
                                    'query': transcript
                                })
                                
                                context = await rag_service.query_context(
                                    query_text=transcript,
                                    namespace=namespace,
                                    top_k=5
                                )
                                logger.info(f"Test conversation - Retrieved context (length: {len(context)})")
                                
                                # Count chunks
                                chunk_count = context.count('[Result') if context else 0
                                await websocket.send_json({
                                    'type': 'rag_result',
                                    'chunks': chunk_count,
                                    'context_length': len(context)
                                })
                                
                                # Inject context into conversation
                                if context and "No relevant context" not in context:
                                    await openai_service.inject_context_to_conversation(openai_ws, context)
                                    
                            except Exception as e:
                                logger.error(f"Test conversation - Error querying Pinecone: {e}")
                                await websocket.send_json({
                                    'type': 'error',
                                    'error': f'RAG query failed: {str(e)}'
                                })
                    
                    # Handle response completion
                    elif event_type == 'response.done':
                        logger.info("Test conversation: Response completed")
                        await websocket.send_json({
                            'type': 'response_done'
                        })
                    
                    # Handle errors
                    elif event_type == 'error':
                        error_info = data.get('error', {})
                        logger.error(f"Test conversation - OpenAI error: {error_info}")
                        await websocket.send_json({
                            'type': 'error',
                            'error': str(error_info)
                        })
                        
            except Exception as e:
                logger.error(f"Error in openai_to_browser: {e}")
                import traceback
                logger.error(traceback.format_exc())
                try:
                    await websocket.send_json({
                        'type': 'error',
                        'error': str(e)
                    })
                except:
                    pass
        
        # Run both tasks concurrently
        logger.info("Test conversation: Starting concurrent tasks")
        try:
            await asyncio.gather(
                browser_to_openai(),
                openai_to_browser(),
                return_exceptions=True
            )
        except Exception as e:
            logger.error(f"Error in gather: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
    except Exception as e:
        logger.error(f"Test conversation WebSocket error: {e}")
        try:
            await websocket.send_json({
                'type': 'error',
                'error': str(e)
            })
        except:
            pass
    finally:
        # Clean up
        if openai_ws:
            await openai_ws.close()
            logger.info("Test conversation: OpenAI WebSocket closed")
        
        try:
            await websocket.close()
            logger.info("Test conversation: Browser WebSocket closed")
        except:
            pass
        
        logger.info(f"Test conversation session ended")

# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content=ApiError(error=exc.detail).dict()
    )

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content=ApiError(
            error="Internal server error",
            details=str(exc)
        ).dict()
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
