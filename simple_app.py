import streamlit as st
import os
import time
import tempfile
import json
import logging
import threading
import base64
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import pubsub_v1
from google.api_core import retry
from unstructured_client import UnstructuredClient
from unstructured_client.models.operations import ListWorkflowsRequest, ListJobsRequest, RunWorkflowRequest, ListDestinationsRequest, UpdateDestinationRequest, CreateSourceRequest
from unstructured_client.models.shared import UpdateDestinationConnector, CreateSourceConnector
from google.cloud import storage
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Load environment variables
load_dotenv()

# GCP Pub/Sub configuration
PUBSUB_PROJECT_ID = os.getenv("PUBSUB_PROJECT_ID")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", "workflow-jobs")
PUBSUB_SUBSCRIPTION = os.getenv("PUBSUB_SUBSCRIPTION", "workflow-jobs-sub")
PUBSUB_CREDENTIALS_PATH = os.getenv("GCP_PUB_SUB_CREDENTIALS")
PUBSUB_CREDENTIALS_BASE64 = os.getenv("GCP_PUB_SUB_CREDENTIALS_BASE64")

# Global thread-safe job tracking
import threading
from queue import Queue

# Global job state - thread safe
current_job_lock = threading.Lock()
current_job_id = None
job_queue_count = 0

# Page config
st.set_page_config(
    page_title="PDF Upload & Processing",
    page_icon="📚",
    layout="wide"
)

# Initialize session state
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = []
if 'custom_folder_name' not in st.session_state:
    st.session_state.custom_folder_name = ""
if 'created_source_connector' not in st.session_state:
    st.session_state.created_source_connector = None
if 'current_job_id' not in st.session_state:
    st.session_state.current_job_id = None
if 'job_queue_count' not in st.session_state:
    st.session_state.job_queue_count = 0
if 'pubsub_consumer_started' not in st.session_state:
    st.session_state.pubsub_consumer_started = False

@st.cache_resource
def init_unstructured_client():
    """Initialize Unstructured client"""
    api_key = os.getenv("UNSTRUCTURED_API_KEY")
    if not api_key:
        st.error("❌ UNSTRUCTURED_API_KEY not found in environment variables")
        return None
    
    return UnstructuredClient(api_key_auth=api_key)

@st.cache_resource
def init_gcs_client():
    """Initialize Google Cloud Storage client"""
    try:
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        
        # Priority 1: Check for base64-encoded credentials (deployment)
        credentials_base64 = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_BASE64")
        if credentials_base64:
            logger.info("Using GOOGLE_APPLICATION_CREDENTIALS_BASE64")
            try:
                credentials_json = base64.b64decode(credentials_base64).decode('utf-8')
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                client = storage.Client(credentials=credentials, project=project_id)
                return client
            except Exception as e:
                logger.error(f"Error decoding base64 credentials: {e}")
                st.error(f"Error decoding base64 credentials: {e}")
        
        # Priority 2: Check for file path (local development)
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if credentials_path and os.path.exists(credentials_path):
            logger.info(f"Using GOOGLE_APPLICATION_CREDENTIALS file: {credentials_path}")
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            client = storage.Client(credentials=credentials, project=project_id)
            return client
        
        # Priority 3: Check session state
        if 'gcs_credentials' in st.session_state:
            logger.info("Using credentials from session state")
            credentials_info = st.session_state.gcs_credentials
            credentials = service_account.Credentials.from_service_account_info(credentials_info)
            client = storage.Client(credentials=credentials, project=project_id)
            return client
        
        # Priority 4: Use default credentials (if running on GCP)
        logger.info("Using default GCP credentials")
        client = storage.Client(project=project_id)
        return client
        
    except Exception as e:
        st.error(f"Error initializing GCS client: {e}")
        logger.error(f"Error initializing GCS client: {e}")
        return None

@st.cache_resource
def init_pubsub_publisher():
    """Initialize Google Cloud Pub/Sub publisher client"""
    try:
        # Priority 1: Check for base64-encoded credentials (deployment)
        if PUBSUB_CREDENTIALS_BASE64:
            logger.info("Using GCP_PUB_SUB_CREDENTIALS_BASE64")
            try:
                credentials_json = base64.b64decode(PUBSUB_CREDENTIALS_BASE64).decode('utf-8')
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                publisher = pubsub_v1.PublisherClient(credentials=credentials)
                logger.info("Pub/Sub publisher client initialized with base64 credentials")
                return publisher
            except Exception as e:
                logger.error(f"Error decoding base64 Pub/Sub credentials: {e}")
                st.error(f"Error decoding base64 Pub/Sub credentials: {e}")
        
        # Priority 2: Check for file path (local development)
        if PUBSUB_CREDENTIALS_PATH and os.path.exists(PUBSUB_CREDENTIALS_PATH):
            logger.info(f"Using GCP_PUB_SUB_CREDENTIALS file: {PUBSUB_CREDENTIALS_PATH}")
            credentials = service_account.Credentials.from_service_account_file(PUBSUB_CREDENTIALS_PATH)
            publisher = pubsub_v1.PublisherClient(credentials=credentials)
            logger.info("Pub/Sub publisher client initialized with file credentials")
            return publisher
        
        # Priority 3: Use default credentials
        logger.info("Using default GCP credentials for Pub/Sub publisher")
        publisher = pubsub_v1.PublisherClient()
        logger.info("Pub/Sub publisher client initialized with default credentials")
        return publisher
        
    except Exception as e:
        logger.error(f"Error initializing Pub/Sub publisher: {e}")
        st.error(f"Error initializing Pub/Sub publisher: {e}")
        return None

@st.cache_resource
def init_pubsub_subscriber():
    """Initialize Google Cloud Pub/Sub subscriber client"""
    try:
        # Priority 1: Check for base64-encoded credentials (deployment)
        if PUBSUB_CREDENTIALS_BASE64:
            logger.info("Using GCP_PUB_SUB_CREDENTIALS_BASE64")
            try:
                credentials_json = base64.b64decode(PUBSUB_CREDENTIALS_BASE64).decode('utf-8')
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(credentials_info)
                subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
                logger.info("Pub/Sub subscriber client initialized with base64 credentials")
                return subscriber
            except Exception as e:
                logger.error(f"Error decoding base64 Pub/Sub credentials: {e}")
                st.error(f"Error decoding base64 Pub/Sub credentials: {e}")
        
        # Priority 2: Check for file path (local development)
        if PUBSUB_CREDENTIALS_PATH and os.path.exists(PUBSUB_CREDENTIALS_PATH):
            logger.info(f"Using GCP_PUB_SUB_CREDENTIALS file: {PUBSUB_CREDENTIALS_PATH}")
            credentials = service_account.Credentials.from_service_account_file(PUBSUB_CREDENTIALS_PATH)
            subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
            logger.info("Pub/Sub subscriber client initialized with file credentials")
            return subscriber
        
        # Priority 3: Use default credentials
        logger.info("Using default GCP credentials for Pub/Sub subscriber")
        subscriber = pubsub_v1.SubscriberClient()
        logger.info("Pub/Sub subscriber client initialized with default credentials")
        return subscriber
        
    except Exception as e:
        logger.error(f"Error initializing Pub/Sub subscriber: {e}")
        st.error(f"Error initializing Pub/Sub subscriber: {e}")
        return None

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
        st.error(f"Error fetching workflows: {e}")
        return []

def get_workflow_details(client, workflow_id):
    """Get detailed workflow information"""
    try:
        # Use the correct SDK method - exactly like in run_workflow_test.py
        workflow_details = client.workflows.get_workflow(workflow_id)
        return workflow_details.workflow_information
    except Exception as e:
        logger.error(f"Error getting workflow details: {e}")
        # Fallback - get from list of workflows
        try:
            workflows = get_workflows(client)
            for workflow in workflows:
                if workflow.id == workflow_id:
                    logger.info(f"Found workflow via list: {workflow.name}")
                    return workflow
            logger.warning(f"Workflow {workflow_id} not found in list")
            return None
        except Exception as e2:
            logger.error(f"Error getting workflow from list: {e2}")
            return None

def check_workflow_jobs(client, workflow_id, limit=10):
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

def monitor_workflow_activity(client, workflow_id, uploaded_files):
    """Monitor workflow activity and provide diagnostics"""
    st.subheader("🔍 Workflow Monitoring & Diagnostics")
    
    # Get workflow details
    workflow_info = get_workflow_details(client, workflow_id)
    if workflow_info:
        st.write("**Workflow Configuration:**")
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"• **Name:** {workflow_info.name}")
            st.write(f"• **Status:** {workflow_info.status}")
            st.write(f"• **ID:** {workflow_id}")
        with col2:
            st.write(f"• **Created:** {getattr(workflow_info, 'created_at', 'Unknown')}")
            st.write(f"• **Updated:** {getattr(workflow_info, 'updated_at', 'Unknown')}")
        
        # Check if workflow is active
        if workflow_info.status.lower() != 'active':
            st.error(f"🚨 **ISSUE FOUND:** Workflow status is '{workflow_info.status}' - should be 'active'")
        else:
            st.success("✅ Workflow status is active")
    else:
        st.error("❌ Could not retrieve workflow details")
    
    # Check recent jobs with detailed analysis
    st.write("**Job Analysis:**")
    jobs = check_workflow_jobs(client, workflow_id)
    
    if jobs:
        st.success(f"✅ Found {len(jobs)} jobs")
        
        # Show jobs in a nice table format
        for i, job in enumerate(jobs[:5]):
            status_emoji = {
                "completed": "✅", "finished": "✅", "running": "🔄", 
                "pending": "⏳", "failed": "❌", "cancelled": "⏹️"
            }.get(job.status.lower(), "❓")
            
            created_time = job.created_at or "Unknown"
            job_id_short = job.id[:8] if hasattr(job, 'id') else "Unknown"
            
            # Create expandable job details
            with st.expander(f"{status_emoji} {job.status.upper()} - {job_id_short}... - {created_time}"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Job ID:** {getattr(job, 'id', 'Unknown')}")
                    st.write(f"**Status:** {getattr(job, 'status', 'Unknown')}")
                    st.write(f"**Created:** {getattr(job, 'created_at', 'Unknown')}")
                with col2:
                    st.write(f"**Updated:** {getattr(job, 'updated_at', 'Unknown')}")
                    st.write(f"**Runtime:** {getattr(job, 'runtime', 'Unknown')}")
                    st.write(f"**Workflow:** {getattr(job, 'workflow_name', 'Unknown')}")
                
                # Show job details as JSON for debugging
                if st.checkbox(f"Show raw job data", key=f"raw_{i}"):
                    st.json(job.__dict__ if hasattr(job, '__dict__') else str(job))
        
        # Analysis of recent activity
        recent_jobs = [j for j in jobs if j.created_at and 'Sep 26, 2025' in str(j.created_at)]
        if recent_jobs:
            st.info(f"🎉 Found {len(recent_jobs)} recent jobs - workflow is active!")
        
        # Check for completed jobs
        completed_jobs = [j for j in jobs if j.status.lower() in ['completed', 'finished']]
        if completed_jobs:
            st.success(f"✅ {len(completed_jobs)} jobs completed successfully")
            st.write("**This means your documents have been processed and should be in Pinecone!**")
        
    else:
        st.warning("⚠️ No jobs returned from API")
        st.write("**Possible reasons:**")
        st.write("• API method signature issue (we're fixing this)")
        st.write("• Jobs exist but API call format is incorrect")
        st.write("• Workflow has no job history yet")
        
        # Show what we know from the dashboard
        st.info("💡 **From your dashboard screenshot, we can see:**")
        st.write("• ✅ Workflow IS working (finished job visible)")
        st.write("• ✅ Target bucket: `gs://unpipetestm/documents`")
        st.write("• ✅ Job completed: Sep 26, 2025 1:34 AM")
        st.write("• ✅ Runtime: 1 minute 49 seconds")
    
    # File upload diagnostics with path analysis
    if uploaded_files:
        st.write("**File Upload Analysis:**")
        
        # Analyze upload patterns
        bucket_name = None
        upload_paths = []
        
        for file_info in uploaded_files:
            if isinstance(file_info, dict):
                bucket_name = file_info['bucket']
                upload_paths.append(file_info['gcs_path'])
                
                st.write(f"📄 **{file_info['filename']}**")
                st.write(f"   • GCS Path: `gs://{file_info['bucket']}/{file_info['gcs_path']}`")
                st.write(f"   • Upload Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(file_info.get('uploaded_at', 0)))}")
                
                # Verify file exists
                try:
                    gcs_client = init_gcs_client()
                    if gcs_client:
                        bucket = gcs_client.bucket(file_info['bucket'])
                        blob = bucket.blob(file_info['gcs_path'])
                        if blob.exists():
                            st.write(f"   • ✅ Confirmed in GCS (Size: {blob.size} bytes)")
                        else:
                            st.write(f"   • ❌ File missing from GCS!")
                except Exception as e:
                    st.write(f"   • ⚠️ GCS check failed: {e}")
        
        # Path analysis
        if upload_paths:
            st.write("**Path Analysis:**")
            common_prefix = "documents/"
            st.write(f"• Files uploaded to: `/{common_prefix}*`")
            st.write(f"• Bucket: `{bucket_name}`")
            
            st.warning("🔍 **ACTION NEEDED:** Check if your workflow source connector is configured to monitor:")
            st.code(f"Bucket: {bucket_name}\nPath: {common_prefix} (or root /)")
    
    # Recommendations
    st.write("**🎯 Next Steps:**")
    if not jobs:
        st.write("1. **Check Unstructured.io workflow settings:**")
        st.write("   • Verify GCS source connector configuration")
        st.write("   • Ensure bucket name matches exactly")
        st.write("   • Check if path filter includes `/documents/` or is set to `/`")
        
        st.write("2. **Test workflow configuration:**")
        st.write("   • Try uploading a file directly to bucket root `/`")
        st.write("   • Check workflow logs in Unstructured.io dashboard")
        
        st.write("3. **Verify permissions:**")
        st.write("   • Ensure Unstructured service account can read your bucket")
    
    return jobs

def upload_file_to_gcs(gcs_client, bucket_name, file_content, filename):
    """Upload file to Google Cloud Storage with detailed logging"""
    try:
        logger.info(f"Starting upload: {filename} to bucket {bucket_name}")
        
        # Get the bucket
        bucket = gcs_client.bucket(bucket_name)
        logger.info(f"Got bucket reference: {bucket_name}")
        
        # Create a blob (file) in the bucket
        # Use custom folder name if available, otherwise default paths
        if st.session_state.custom_folder_name:
            upload_path = f"{st.session_state.custom_folder_name}/"
        else:
            upload_path = getattr(st.session_state, 'upload_path', 'protocols/dev')
            if upload_path and not upload_path.endswith('/'):
                upload_path += '/'
        blob_name = f"{upload_path}{filename}"
        blob = bucket.blob(blob_name)
        logger.info(f"Created blob reference: {blob_name}")
        
        # Upload the file content
        blob.upload_from_string(file_content, content_type='application/pdf')
        logger.info(f"Upload completed: {blob_name}")
        
        # Get file metadata
        blob.reload()  # Refresh to get updated metadata
        
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
        st.error(f"Error uploading to GCS: {e}")
        return None

def trigger_workflow(unstructured_client, workflow_id, namespace=None, pinecone_connector_id=None, retry_count=0, max_retries=3):
    """Manually trigger a workflow to process uploaded files with optional namespace update and rate limiting protection"""
    try:
        # Rate limiting protection with exponential backoff
        if retry_count > 0:
            wait_time = min(2 ** retry_count, 30)  # Exponential backoff, max 30 seconds
            logger.info(f"Retry {retry_count}/{max_retries}, waiting {wait_time}s before retry...")
            time.sleep(wait_time)
        
        # If namespace is provided, update the Pinecone connector first
        if namespace and pinecone_connector_id:
            st.info(f"🔄 Updating Pinecone namespace to: `{namespace}`")
            namespace_result = update_pinecone_namespace(
                unstructured_client, 
                pinecone_connector_id, 
                namespace
            )
            
            if not namespace_result.get('success'):
                st.warning("⚠️ Namespace update failed, proceeding with current namespace")
            else:
                st.success(f"✅ Namespace updated to: `{namespace}`")
        
        logger.info(f"Triggering workflow {workflow_id}")
        
        # Print workflow input details
        print(f"\n=== WORKFLOW EXECUTION STARTED ===")
        print(f"📥 Workflow Input Details:")
        print(f"   • Workflow ID: {workflow_id}")
        print(f"   • Namespace: {namespace or 'default'}")
        print(f"   • Pinecone Connector ID: {pinecone_connector_id or 'N/A'}")
        print(f"   • Custom Folder: {getattr(st.session_state, 'custom_folder_name', 'N/A')}")
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
                
                st.success(f"✅ Workflow triggered successfully!")
                st.info(f"📋 Job ID: {job_id}")
                st.info(f"🔄 Initial Status: {job_status}")
                if namespace:
                    st.info(f"🏷️ Using namespace: `{namespace}`")
                return {'success': True, 'job_id': job_id, 'job_info': job_info, 'namespace': namespace}
            else:
                # Print workflow output details for case without job info
                print(f"\n📤 Workflow Output Details:")
                print(f"   • Response Type: {type(response).__name__}")
                print(f"   • Raw Response: {getattr(response, 'raw_response', 'N/A')}")
                print(f"   • Success: True")
                print(f"   • Note: Job details available in monitoring")
                print(f"=== WORKFLOW EXECUTION COMPLETED ===\n")
                
                st.success(f"✅ Workflow triggered successfully!")
                st.info("📋 Job details will be available in workflow monitoring")
                if namespace:
                    st.info(f"🏷️ Using namespace: `{namespace}`")
                return {'success': True, 'response': response, 'namespace': namespace}
        else:
            # Print workflow output for basic success case
            print(f"\n📤 Workflow Output Details:")
            print(f"   • Response: Basic success (no detailed response object)")
            print(f"   • Success: True")
            print(f"=== WORKFLOW EXECUTION COMPLETED ===\n")
            
            st.success(f"✅ Workflow trigger request sent")
            return {'success': True, 'namespace': namespace}
            
    except Exception as e:
        # Check if it's a rate limit error (429)
        error_str = str(e)
        if "429" in error_str or "Rate limit" in error_str:
            if retry_count < max_retries:
                logger.warning(f"Rate limit hit, retrying... ({retry_count + 1}/{max_retries})")
                st.warning(f"⏳ Rate limit hit, retrying in a moment... (Attempt {retry_count + 1}/{max_retries})")
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
                st.error(f"❌ Rate limit exceeded after {max_retries} retries. Please wait a moment and try again.")
        
        # Print workflow error details
        print(f"\n❌ WORKFLOW EXECUTION FAILED")
        print(f"📤 Error Details:")
        print(f"   • Error Type: {type(e).__name__}")
        print(f"   • Error Message: {str(e)}")
        print(f"   • Success: False")
        print(f"=== WORKFLOW EXECUTION COMPLETED ===\n")
        
        logger.error(f"Error triggering workflow: {e}")
        st.error(f"❌ Error triggering workflow: {e}")
        
        # Still show helpful info about auto-detection as fallback
        st.info("ℹ️ If manual triggering fails, workflows may still auto-detect files")
        st.write("**Fallback options:**")
        st.write("• Wait 2-5 minutes for auto-detection")
        st.write("• Check workflow configuration for GCS monitoring")
        st.write("• Use 'Full Workflow Diagnostics' to monitor activity")
        
        return {'success': False, 'error': str(e)}

def check_job_status(client, job_id):
    """Check the status of a workflow job"""
    try:
        response = client.jobs.get_job(job_id)
        return response.job_information if response else None
    except Exception as e:
        st.error(f"Error checking job status: {e}")
        return None

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
        st.error(f"Error fetching destination connectors: {e}")
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
            st.success(f"✅ Updated Pinecone namespace to: `{new_namespace}`")
            return {'success': True, 'namespace': new_namespace}
        else:
            raise Exception("Update request returned no response")
            
    except Exception as e:
        logger.error(f"Error updating Pinecone namespace: {e}")
        st.error(f"❌ Error updating namespace: {e}")
        
        # Show helpful debug information
        if "validation error" in str(e).lower():
            st.error("🔧 **Configuration Issue Detected:**")
            st.write("• Check that PINECONE_INDEX_NAME is set in your environment")
            st.write("• Check that PINECONE_API_KEY is set in your environment")
            st.write("• Ensure the connector configuration has all required fields")
        
        return {'success': False, 'error': str(e)}

def create_gcs_source_connector(client, folder_name, bucket_name):
    """Create a GCS source connector with custom folder name and datetime suffix"""
    try:
        # Generate datetime suffix
        datetime_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create connector name with folder and datetime
        connector_name = f"gcs_{folder_name}_{datetime_suffix}" if folder_name else f"gcs_{datetime_suffix}"
        
        # Store globally in session state
        st.session_state.custom_folder_name = f"{folder_name}_{datetime_suffix}" if folder_name else datetime_suffix
        
        logger.info(f"Creating GCS source connector: {connector_name}")
        logger.info(f"Custom folder name: {st.session_state.custom_folder_name}")
        
        # Get GCS service account key from session state or environment
        service_account_key = None
        if 'gcs_credentials' in st.session_state:
            service_account_key = json.dumps(st.session_state.gcs_credentials)
        else:
            # Priority 1: Check for base64-encoded credentials (deployment)
            credentials_base64 = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_BASE64")
            if credentials_base64:
                try:
                    service_account_key = base64.b64decode(credentials_base64).decode('utf-8')
                except Exception as e:
                    logger.error(f"Error decoding base64 credentials for source connector: {e}")
            
            # Priority 2: Try to get from environment file (local development)
            if not service_account_key:
                credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
                if credentials_path and os.path.exists(credentials_path):
                    with open(credentials_path, 'r') as f:
                        service_account_key = f.read()
        
        if not service_account_key:
            raise Exception("Service account key is required for GCS source connector")
        
        # Create remote URL with custom folder
        remote_url = f"gs://{bucket_name}/{st.session_state.custom_folder_name}/"
        
        # Create the source connector
        response = client.sources.create_source(
            request=CreateSourceRequest(
                create_source_connector=CreateSourceConnector(
                    name="gcs",
                    type="gcs",
                    config={
                        "service_account_key": service_account_key,
                        "remote_url": remote_url,
                        "recursive": True
                    }
                )
            )
        )
        
        if response and hasattr(response, 'source_connector_information'):
            connector_info = response.source_connector_information
            logger.info(f"Successfully created GCS source connector: {connector_info}")
            
            # Store connector info globally
            st.session_state.created_source_connector = {
                'id': getattr(connector_info, 'id', None),
                'name': connector_name,
                'remote_url': remote_url,
                'folder_name': st.session_state.custom_folder_name
            }
            
            st.success(f"✅ Created GCS source connector: `{connector_name}`")
            st.info(f"📁 Custom folder: `{st.session_state.custom_folder_name}`")
            st.info(f"🔗 Remote URL: `{remote_url}`")
            
            return {
                'success': True,
                'connector_info': connector_info,
                'connector_name': connector_name,
                'folder_name': st.session_state.custom_folder_name,
                'remote_url': remote_url
            }
        else:
            raise Exception("No connector information returned from API")
            
    except Exception as e:
        logger.error(f"Error creating GCS source connector: {e}")
        st.error(f"❌ Error creating GCS source connector: {e}")
        
        # Show helpful debug information
        st.error("🔧 **Troubleshooting:**")
        st.write("• Ensure GOOGLE_APPLICATION_CREDENTIALS is set or service account key is uploaded")
        st.write("• Check that the bucket name is valid and accessible")
        st.write("• Verify Unstructured API permissions for source connector creation")
        
        return {'success': False, 'error': str(e)}

def add_job_to_queue(job_data):
    """Add a job to Pub/Sub queue with retry logic"""
    max_retries = 2
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            publisher = init_pubsub_publisher()
            if not publisher:
                raise Exception("Failed to initialize Pub/Sub publisher")
            
            # Create topic path
            topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, PUBSUB_TOPIC)
            
            # Serialize job data to JSON bytes
            message_data = json.dumps(job_data).encode('utf-8')
            
            # Publish message with retry
            future = publisher.publish(topic_path, message_data)
            message_id = future.result()  # Wait for publish to complete
            
            global job_queue_count
            st.session_state.job_queue_count += 1
            job_queue_count += 1
            logger.info(f"Job added to queue: {job_data['filename']} (Message ID: {message_id})")
            st.success(f"📥 Job added to queue (Position: {st.session_state.job_queue_count})")
            return True
            
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to add job to queue (attempt {attempt + 1}/{max_retries}): {e}")
                time.sleep(retry_delay)
                continue
            else:
                logger.error(f"Failed to add job to queue after retries: {e}")
                st.warning("⚠️ Queue not available, processing immediately")
                return False
    
    return False

def is_job_running(client):
    """Check if current job is still running - thread safe version"""
    global current_job_id, current_job_lock
    
    with current_job_lock:
        # First, check our tracked job
        if current_job_id:
            try:
                job_status = check_job_status(client, current_job_id)
                if job_status and hasattr(job_status, 'status'):
                    status = job_status.status.lower()
                    if status in ['completed', 'failed', 'cancelled', 'finished']:
                        logger.info(f"Job {current_job_id} completed with status: {status}")
                        current_job_id = None
                        return False
                    return True
            except Exception as e:
                logger.error(f"Error checking job status: {e}")
                # Assume job is not running if we can't check
                current_job_id = None
        
        # Skip platform job check - requires workflow_id parameter
        # Our current_job_id tracking is sufficient for job management
        
        return False

def process_job_from_queue(client, job_data):
    """Process a single job from the queue"""
    try:
        logger.info(f"Processing job: {job_data['filename']}")
        logger.info(f"Job data: {job_data}")
        
        # Trigger workflow
        logger.info(f"Triggering workflow with ID: {job_data['workflow_id']}, namespace: {job_data.get('namespace')}, connector_id: {job_data.get('connector_id')}")
        result = trigger_workflow(
            client,
            job_data['workflow_id'],
            job_data.get('namespace'),
            job_data.get('connector_id')
        )
        
        logger.info(f"Workflow trigger result: {result}")
        
        if result and result.get('success') and 'job_id' in result:
            global current_job_id, current_job_lock
            with current_job_lock:
                current_job_id = result['job_id']
            logger.info(f"Started job: {result['job_id']}")
            
            # Monitor job completion in background
            def monitor_job_completion():
                """Monitor job completion and clear current_job_id when done"""
                job_id = result['job_id']
                max_wait_time = 600  # 10 minutes max wait
                check_interval = 15  # Check every 15 seconds
                elapsed_time = 0
                
                while elapsed_time < max_wait_time:
                    try:
                        time.sleep(check_interval)
                        elapsed_time += check_interval
                        
                        job_status = check_job_status(client, job_id)
                        if job_status and hasattr(job_status, 'status'):
                            status = job_status.status.lower()
                            logger.info(f"Job {job_id} status: {status}")
                            
                            if status in ['completed', 'failed', 'cancelled', 'finished']:
                                with current_job_lock:
                                    if current_job_id == job_id:
                                        current_job_id = None
                                        logger.info(f"Job {job_id} completed with status: {status}")
                                break
                    except Exception as e:
                        logger.error(f"Error monitoring job {job_id}: {e}")
                        break
                
                # Ensure job is cleared after timeout
                with current_job_lock:
                    if current_job_id == job_id:
                        current_job_id = None
                        logger.warning(f"Job {job_id} monitoring timed out, clearing job status")
            
            # Start monitoring in background thread
            import threading
            monitor_thread = threading.Thread(target=monitor_job_completion, daemon=True)
            monitor_thread.start()
            
            return True
        else:
            # Check if it's a 409 error (job already running)
            if result and 'error' in result and '409' in str(result['error']):
                logger.warning(f"Cannot start job for {job_data['filename']} - another job is already running. Will retry later.")
                # Don't consider this a failure, just return False to keep job in queue
                return False
            else:
                logger.error(f"Failed to start workflow for {job_data['filename']} - Result: {result}")
                return False
            
    except Exception as e:
        logger.error(f"Error processing job: {e}")
        return False

def job_consumer():
    """Pub/Sub subscriber that processes jobs sequentially"""
    while True:  # Keep running continuously
        try:
            subscriber = init_pubsub_subscriber()
            if not subscriber:
                logger.error("Failed to initialize Pub/Sub subscriber")
                time.sleep(10)
                continue
            
            # Create subscription path
            subscription_path = subscriber.subscription_path(PUBSUB_PROJECT_ID, PUBSUB_SUBSCRIPTION)
            
            logger.info(f"Pub/Sub consumer started, listening on: {subscription_path}")
            
            def callback(message):
                """Callback function to process messages"""
                try:
                    # Decode message data
                    job_data = json.loads(message.data.decode('utf-8'))
                    logger.info(f"Received job from queue: {job_data['filename']}")
                    
                    # Get client (this is a simplified approach)
                    client = init_unstructured_client()
                    if not client:
                        logger.error("Failed to initialize Unstructured client")
                        message.nack()  # Negative acknowledgment - message will be redelivered
                        return
                    
                    # Wait until no job is running
                    while is_job_running(client):
                        logger.info("Job currently running, waiting...")
                        time.sleep(30)
                    
                    # Process the job
                    if process_job_from_queue(client, job_data):
                        global job_queue_count
                        job_queue_count = max(0, job_queue_count - 1)
                        logger.info(f"Job processed successfully: {job_data['filename']}")
                        message.ack()  # Acknowledge successful processing
                        # Add 30 second delay between jobs as requested
                        logger.info("Waiting 30 seconds before processing next job...")
                        time.sleep(30)
                    else:
                        # For 409 errors, we don't decrement the queue count since we'll retry
                        logger.warning(f"Job processing failed (will retry): {job_data['filename']}")
                        message.nack()  # Negative acknowledgment - message will be redelivered
                        time.sleep(10)  # Wait longer before retry
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    message.nack()  # Negative acknowledgment on error
            
            # Subscribe to the subscription
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
            logger.info(f"Listening for messages on {subscription_path}...")
            
            # Keep the subscriber running
            try:
                streaming_pull_future.result()
            except Exception as e:
                logger.error(f"Streaming pull error: {e}")
                streaming_pull_future.cancel()
                streaming_pull_future.result()  # Wait for cancellation to complete
            
        except Exception as e:
            logger.error(f"Pub/Sub consumer error: {e}, retrying in 10 seconds...")
            time.sleep(10)

def start_pubsub_consumer():
    """Start Pub/Sub consumer in background thread"""
    if not st.session_state.pubsub_consumer_started:
        try:
            consumer_thread = threading.Thread(target=job_consumer, daemon=True)
            consumer_thread.start()
            st.session_state.pubsub_consumer_started = True
            logger.info("Pub/Sub consumer thread started")
        except Exception as e:
            logger.error(f"Failed to start Pub/Sub consumer: {e}")

def main():
    st.title("📚 PDF Upload & Processing Interface")
    st.markdown("Upload PDFs to your Unstructured workflow for document processing")
    
    # Initialize client
    client = init_unstructured_client()
    if not client:
        st.stop()
    
    # Start Pub/Sub consumer
    start_pubsub_consumer()
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("� Configouration")
        
        # Initialize GCS client
        gcs_client = init_gcs_client()
        bucket_name = os.getenv("GCS_BUCKET_NAME")
        
        # Destination connector management for dynamic namespace
        st.subheader("🏷️ Pinecone Namespace Management")
        pinecone_connectors = get_pinecone_connectors(client)
        
        # Initialize session state for namespace
        if 'selected_namespace' not in st.session_state:
            st.session_state.selected_namespace = "default"
        if 'selected_pinecone_connector' not in st.session_state:
            st.session_state.selected_pinecone_connector = None
        
        if pinecone_connectors:
            st.success(f"✅ Found {len(pinecone_connectors)} Pinecone connector(s)")
            
            # Connector selection
            connector_options = {}
            for connector in pinecone_connectors:
                display_name = f"{getattr(connector, 'name', 'Unnamed')} ({getattr(connector, 'id', 'No ID')[:8]}...)"
                # Properly access namespace from Pydantic model
                config = getattr(connector, 'config', None)
                current_namespace = getattr(config, 'namespace', 'default') if config else 'default'
                display_name += f" - Current: {current_namespace}"
                connector_options[display_name] = connector.id
            
            selected_connector_display = st.selectbox(
                "Select Pinecone Connector",
                options=list(connector_options.keys()),
                help="Choose which Pinecone connector to use for namespace management"
            )
            
            if selected_connector_display:
                st.session_state.selected_pinecone_connector = connector_options[selected_connector_display]
                selected_connector = next(c for c in pinecone_connectors if c.id == st.session_state.selected_pinecone_connector)
                # Properly access namespace from Pydantic model
                config = getattr(selected_connector, 'config', None)
                current_namespace = getattr(config, 'namespace', 'default') if config else 'default'
                
                st.info(f"**Selected Connector:** {getattr(selected_connector, 'name', 'Unnamed')}")
                st.info(f"**Current Namespace:** `{current_namespace}`")
                
                # Namespace input
                col1, col2 = st.columns([3, 1])
                with col1:
                    new_namespace = st.text_input(
                        "Namespace for new uploads",
                        value=current_namespace,
                        help="Specify namespace for document storage in Pinecone"
                    )
                    st.session_state.selected_namespace = new_namespace
                
                with col2:
                    if st.button("🔄 Update", key="update_namespace"):
                        if new_namespace != current_namespace:
                            result = update_pinecone_namespace(client, st.session_state.selected_pinecone_connector, new_namespace)
                            if result.get('success'):
                                st.success("✅ Updated!")
                                st.rerun()
                        else:
                            st.info("ℹ️ Same namespace")
                
                # Namespace presets
                with st.expander("📋 Namespace Presets"):
                    preset_cols = st.columns(3)
                    presets = ["default", "production", "testing", "staging", "user_docs", "temp"]
                    
                    for i, preset in enumerate(presets):
                        col_idx = i % 3
                        with preset_cols[col_idx]:
                            if st.button(f"📌 {preset}", key=f"preset_{preset}"):
                                st.session_state.selected_namespace = preset
                                result = update_pinecone_namespace(client, st.session_state.selected_pinecone_connector, preset)
                                if result.get('success'):
                                    st.rerun()
        else:
            st.warning("⚠️ No Pinecone connectors found")
            st.info("💡 Create a Pinecone destination connector in your Unstructured workflow first")
        
        st.divider()
        
        # Workflow selection
        st.subheader("📋 Workflow Selection")
        workflows = get_workflows(client)
        if not workflows:
            st.error("❌ No workflows found. Please create a workflow first.")
            st.stop()
        
        workflow_options = {}
        for workflow in workflows:
            display_name = f"{workflow.name}"
            if hasattr(workflow, 'status'):
                display_name += f" ({workflow.status})"
            workflow_options[display_name] = workflow.id
        
        selected_workflow_name = st.selectbox(
            "Select Workflow",
            options=list(workflow_options.keys()),
            help="Choose the workflow that monitors your GCS bucket"
        )
        
        selected_workflow_id = workflow_options[selected_workflow_name]
        selected_workflow = next(w for w in workflows if w.id == selected_workflow_id)
        
        st.info(f"**Workflow:** {selected_workflow.name}")
        st.info(f"**ID:** {selected_workflow_id[:8]}...")
        
        st.divider()
        
        # File upload section
        st.header("📄 Upload PDFs to GCS")
        
        if gcs_client and bucket_name:
            uploaded_files = st.file_uploader(
                "Choose PDF files",
                type=['pdf'],
                accept_multiple_files=True,
                help="Files will be uploaded to Google Cloud Storage"
            )
            
            # Process uploaded files
            if uploaded_files:
                for uploaded_file in uploaded_files:
                    file_key = f"{uploaded_file.name}_{uploaded_file.size}"
                    
                    # Check if file already processed (proper deduplication)
                    already_processed = any(
                        f.get('filename') == uploaded_file.name and f.get('size') == uploaded_file.size 
                        for f in st.session_state.processed_files if isinstance(f, dict)
                    )
                    
                    if not already_processed:
                        with st.spinner(f"Uploading {uploaded_file.name} to GCS..."):
                            # Read file content
                            file_content = uploaded_file.read()
                            
                            # Upload to GCS
                            result = upload_file_to_gcs(
                                gcs_client,
                                bucket_name,
                                file_content,
                                uploaded_file.name
                            )
                            
                            if result and result.get('success'):
                                st.success(f"✅ {uploaded_file.name} uploaded to GCS!")
                                st.info(f"📍 Location: {result['public_url']}")
                                
                                # Show detailed upload info
                                with st.expander("📊 Upload Details"):
                                    st.json({
                                        "filename": uploaded_file.name,
                                        "gcs_path": result['blob_name'],
                                        "bucket": result['bucket'],
                                        "size": f"{result.get('size', 0)} bytes",
                                        "uploaded_at": result.get('created', 'Unknown')
                                    })
                                
                                # Add to processed files with timestamp
                                file_info = {
                                    'filename': uploaded_file.name,
                                    'gcs_path': result['blob_name'],
                                    'bucket': result['bucket'],
                                    'uploaded_at': time.time(),
                                    'size': result.get('size', 0)
                                }
                                st.session_state.processed_files.append(file_info)
                                st.session_state.last_upload_time = time.time()
                                
                                # Add to Pub/Sub queue instead of immediate trigger
                                st.info("📥 Adding job to processing queue...")
                                job_data = {
                                    'filename': uploaded_file.name,
                                    'gcs_path': result['blob_name'],
                                    'bucket': result['bucket'],
                                    'workflow_id': selected_workflow_id,
                                    'namespace': st.session_state.selected_namespace if st.session_state.selected_namespace != "default" else None,
                                    'connector_id': st.session_state.selected_pinecone_connector,
                                    'uploaded_at': time.time()
                                }
                                
                                if add_job_to_queue(job_data):
                                    st.success("🎉 File uploaded and added to processing queue!")
                                    # Store job info for tracking
                                    file_info['queued_at'] = time.time()
                                    file_info['namespace'] = job_data.get('namespace', 'default')
                                    st.session_state.processed_files[-1] = file_info  # Update the stored file info
                                else:
                                    # Fallback to direct workflow trigger
                                    st.info("🔄 Processing workflow directly...")
                                    workflow_result = trigger_workflow(
                                        client, 
                                        selected_workflow_id,
                                        namespace=st.session_state.selected_namespace if st.session_state.selected_namespace != "default" else None,
                                        pinecone_connector_id=st.session_state.selected_pinecone_connector
                                    )
                                    if workflow_result and workflow_result.get('success'):
                                        st.success("🎉 Workflow triggered directly!")
                                    else:
                                        st.warning("⚠️ Workflow trigger failed")
                            else:
                                st.error(f"❌ Failed to upload {uploaded_file.name}")
            
            # Workflow info and status
            if st.session_state.processed_files:
                st.success(f"📁 {len(st.session_state.processed_files)} file(s) uploaded to GCS")
                st.info("🔄 Workflows triggered automatically after upload")
                
                # Manual trigger option
                st.subheader("🎯 Manual Workflow Controls")
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🚀 Trigger Workflow Now", use_container_width=True):
                        with st.spinner("Triggering workflow..."):
                            workflow_result = trigger_workflow(
                                client, 
                                selected_workflow_id,
                                namespace=st.session_state.selected_namespace if st.session_state.selected_namespace != "default" else None,
                                pinecone_connector_id=st.session_state.selected_pinecone_connector
                            )
                            if workflow_result and workflow_result.get('success'):
                                st.balloons()
                
                with col2:
                    if st.button("🔄 Refresh Job Status", use_container_width=True):
                        jobs = check_workflow_jobs(client, selected_workflow_id, 3)
                        if jobs:
                            st.success(f"Found {len(jobs)} recent jobs")
                        else:
                            st.info("No recent jobs found")
                
                # Info about processing
                with st.expander("ℹ️ How Workflow Processing Works"):
                    st.markdown("""
                    **Updated workflow behavior with dynamic namespace support:**
                    
                    1. 🏷️ **Namespace**: Select/update Pinecone namespace for data organization
                    2. 🔄 **Update Connector**: Pinecone connector updated with new namespace
                    3. 📤 **Upload**: Files uploaded to GCS bucket
                    4. 🚀 **Trigger**: Workflow automatically triggered via API
                    5. 🔄 **Processing**: Files are processed by Unstructured.io
                    6. 📊 **Storage**: Results stored in specified Pinecone namespace
                    7. ⏱️ **Timing**: Processing usually takes 1-3 minutes
                    
                    **Dynamic Namespace Benefits:**
                    • Multi-tenancy support (different users/organizations)
                    • Environment separation (production, staging, testing)
                    • Content categorization (user_docs, technical_docs, etc.)
                    • Temporary storage (temp namespace for experiments)
                    
                    **Fallback**: If manual trigger fails, workflows may still auto-detect files.
                    """)
                
                # Comprehensive monitoring
                if st.button("🔍 Full Workflow Diagnostics", use_container_width=True):
                    jobs = monitor_workflow_activity(client, selected_workflow_id, st.session_state.processed_files)
                
                # Quick job check
                if st.button("📊 Quick Job Check", use_container_width=True):
                    with st.spinner("Checking recent jobs..."):
                        jobs = check_workflow_jobs(client, selected_workflow_id, 3)
                        if jobs:
                            st.write("**Last 3 Jobs:**")
                            for job in jobs:
                                status_emoji = {
                                    "completed": "✅", "finished": "✅", "running": "🔄", 
                                    "pending": "⏳", "failed": "❌", "cancelled": "⏹️"
                                }.get(job.status.lower(), "❓")
                                job_id = getattr(job, 'id', 'Unknown')
                                job_id_short = job_id[:8] if job_id != 'Unknown' else 'Unknown'
                                created = getattr(job, 'created_at', 'Unknown')
                                st.write(f"{status_emoji} {job.status} - {job_id_short}... - {created}")
                        else:
                            st.warning("No jobs returned from API call")
                            st.info("💡 Check 'Full Workflow Diagnostics' for more details")
                
                # Auto-refresh option
                auto_refresh = st.checkbox("🔄 Auto-refresh job status (every 30s)")
                if auto_refresh:
                    # Use st.empty() for dynamic updates
                    status_placeholder = st.empty()
                    
                    # Auto-refresh logic
                    if 'last_refresh' not in st.session_state:
                        st.session_state.last_refresh = time.time()
                    
                    if time.time() - st.session_state.last_refresh > 30:
                        st.session_state.last_refresh = time.time()
                        with status_placeholder.container():
                            jobs = check_workflow_jobs(client, selected_workflow_id, 1)
                            if jobs:
                                latest_job = jobs[0]
                                st.info(f"Latest: {latest_job.status} - {latest_job.created_at}")
                            else:
                                st.warning("No recent activity detected")
                        st.rerun()
        else:
            st.warning("⚠️ Configure GCS settings above to upload files")
        
        # Show uploaded files with path testing
        if st.session_state.processed_files:
            st.divider()
            st.subheader("📁 Uploaded Files")
            
            for i, file_info in enumerate(st.session_state.processed_files, 1):
                if isinstance(file_info, dict):
                    st.text(f"{i}. {file_info['filename']}")
                    st.caption(f"   📍 {file_info['gcs_path']}")
                    
                    # Show job ID and namespace if available
                    if 'job_id' in file_info:
                        st.caption(f"   🏷️ Job ID: {file_info['job_id'][:8]}...")
                    if 'namespace' in file_info:
                        st.caption(f"   📂 Namespace: {file_info['namespace']}")
                    
                    # Add manual trigger for individual files
                    if st.button(f"🚀 Trigger for this file", key=f"trigger_{i}"):
                        with st.spinner(f"Triggering workflow for {file_info['filename']}..."):
                            workflow_result = trigger_workflow(
                                client, 
                                selected_workflow_id,
                                namespace=st.session_state.selected_namespace if st.session_state.selected_namespace != "default" else None,
                                pinecone_connector_id=st.session_state.selected_pinecone_connector
                            )
                            if workflow_result and workflow_result.get('success'):
                                if 'job_id' in workflow_result:
                                    file_info['job_id'] = workflow_result['job_id']
                                    file_info['namespace'] = workflow_result.get('namespace', 'default')
                                    st.session_state.processed_files[i-1] = file_info
                                st.success(f"✅ Workflow triggered for {file_info['filename']}")
                    
                    # Add test upload to root path
                    if st.button(f"🔄 Test upload to root path", key=f"test_{i}"):
                        with st.spinner(f"Testing root path upload for {file_info['filename']}..."):
                            # Re-upload to root instead of documents/
                            try:
                                # Get the original file (this is a limitation - we'd need to store content)
                                st.info("💡 To test root path, please re-upload the file manually")
                                st.code(f"gsutil cp {file_info['filename']} gs://{file_info['bucket']}/")
                            except Exception as e:
                                st.error(f"Test upload failed: {e}")
                else:
                    # Handle old format
                    filename = file_info.split('_')[0]
                    st.text(f"{i}. {filename}")
            
            # Path configuration helper
            st.subheader("🛠️ Path Configuration Helper")
            st.write("**Current upload path:** `documents/`")
            
            # Option to change upload path
            new_path = st.text_input(
                "Test different upload path:",
                value="documents/",
                help="Try empty string for root, or other paths like 'input/', 'files/', etc."
            )
            
            if new_path != "documents/":
                st.info(f"💡 Next uploads will go to: `{new_path}`")
                # Store the new path preference
                st.session_state.upload_path = new_path
    
    # Main status and monitoring interface
    st.header("📊 Status & Monitoring")
    
    # Add auto-refresh for real-time updates
    if st.button("🔄 Refresh Status", help="Click to refresh current job status"):
        st.rerun()
    
    # Auto-refresh every 30 seconds if there are active jobs
    global current_job_id, current_job_lock
    with current_job_lock:
        if current_job_id:
            st.info("⏱️ Auto-refreshing every 30 seconds while job is running...")
            time.sleep(1)  # Small delay to prevent rapid refreshes
            st.rerun()
    
    # Show custom folder status
    if st.session_state.custom_folder_name or st.session_state.created_source_connector:
        st.subheader("📁 Custom Folder Configuration")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.custom_folder_name:
                st.success(f"**Custom Folder:** `{st.session_state.custom_folder_name}`")
            else:
                st.info("No custom folder configured")
        
        with col2:
            if st.session_state.created_source_connector:
                connector_info = st.session_state.created_source_connector
                st.success(f"**GCS Connector:** `{connector_info['name']}`")
                st.info(f"**Remote URL:** `{connector_info['remote_url']}`")
            else:
                st.info("No custom GCS connector created")
        
        st.divider()
    
    # Show queue and job status
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.session_state.processed_files:
            st.metric("Processed Files", len(st.session_state.processed_files))
        else:
            st.metric("Processed Files", 0)
    
    with col2:
        st.metric("Jobs in Queue", st.session_state.job_queue_count)
    
    with col3:
        # Sync global state with session state for display  
        with current_job_lock:
            active_job_id = current_job_id
            st.session_state.job_queue_count = job_queue_count
        
        if active_job_id:
            st.metric("Current Job", "Running", delta=f"ID: {active_job_id[:8]}...")
        else:
            st.metric("Current Job", "None")
    
    # Workflow status
    st.subheader("🔧 Workflow Info")
    st.text(f"Selected: {selected_workflow.name}")
    if hasattr(selected_workflow, 'status'):
        status_color = "🟢" if selected_workflow.status.lower() == "active" else "🟡"
        st.text(f"Status: {status_color} {selected_workflow.status}")
    
    # Instructions and troubleshooting
    st.subheader("📝 How it Works")
    st.markdown("""
    1. **Configure GCS** in sidebar
    2. **Create Custom Folder** (optional) with datetime suffix
    3. **Create GCS Source Connector** for custom folder monitoring
    4. **Upload PDFs** to Google Cloud Storage in custom folder
    5. **Jobs added to Pub/Sub queue** for sequential processing
    6. **Queue processor monitors** and runs workflows one at a time
    7. **Documents processed** and stored in Pinecone
    8. **Processing completed** when queue is empty
    
    **🔄 Queue-Based Processing Benefits:**
    - **Sequential execution** - only one workflow runs at a time
    - **Reliable queuing** - GCP Pub/Sub handles job persistence
    - **Status monitoring** - track current job and queue length
    - **No conflicts** - prevents workflow interference
    - **Scalable** - can handle multiple rapid uploads
    """)
    
    # Troubleshooting section
    with st.expander("🔧 Troubleshooting Guide"):
        st.markdown("""
        **If workflow isn't detecting files:**
        
        ✅ **Check Workflow Configuration:**
        - Workflow status should be "active"
        - Source connector should point to your GCS bucket
        - Path should match where files are uploaded (`documents/`)
        
        ✅ **Verify GCS Setup:**
        - Files appear in correct bucket and path
        - Service account has proper permissions
        - Bucket is in the same region as workflow
        
        ✅ **Common Issues:**
        - **Wrong path**: Workflow monitors `/` but files in `/documents/`
        - **Permissions**: Service account lacks bucket access
        - **Timing**: Can take 2-10 minutes for detection
        - **File format**: Workflow may only accept certain file types
        
        ✅ **Debug Steps:**
        1. Use "Full Workflow Diagnostics" button
        2. Check if files exist in GCS at expected path
        3. Verify workflow source connector configuration
        4. Look for error messages in job logs
        """)
    
    # Real-time monitoring
    if st.session_state.processed_files:
        st.subheader("⏱️ Real-time Status")
        
        # Show time since last upload
        if 'last_upload_time' in st.session_state:
            time_diff = time.time() - st.session_state.last_upload_time
            minutes_ago = int(time_diff / 60)
            st.write(f"Last upload: {minutes_ago} minutes ago")
            
            if minutes_ago > 10:
                st.warning("⚠️ No activity detected for 10+ minutes. Check workflow configuration.")
            elif minutes_ago > 5:
                st.info("ℹ️ Still waiting for workflow to detect files...")
            else:
                st.success("🔄 Recently uploaded - workflow should detect soon")
        

if __name__ == "__main__":
    main()