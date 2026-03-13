"""
Project Nexus - Core Orchestrator
Event-driven DeFi arbitrage system with swarm intelligence
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import firebase_admin
from firebase_admin import firestore, credentials
from google.cloud import secretmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('nexus.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize Firebase
def initialize_firebase():
    """Initialize Firebase Admin SDK with proper error handling"""
    try:
        # Try to get credentials from environment
        cred_json = os.getenv('FIREBASE_CREDENTIALS_JSON')
        if cred_json:
            cred_dict = json.loads(cred_json)
            cred = credentials.Certificate(cred_dict)
        else:
            # Use default application credentials (GCP)
            cred = credentials.ApplicationDefault()
        
        # Initialize app
        firebase_admin.initialize_app(cred, {
            'projectId': os.getenv('FIREBASE_PROJECT_ID', 'project-nexus')
        })
        
        logger.info("Firebase Admin SDK initialized successfully")
        return firestore.client()
    except Exception as e:
        logger.error(f"Failed to initialize Firebase: {e}")
        raise

# Secret Manager Client
class SecretManager:
    """Secure key management via GCP Secret Manager"""
    
    def __init__(self, project_id: str = None):
        self.project_id = project_id or os.getenv('GCP_PROJECT_ID')
        if not self.project_id:
            raise ValueError("GCP_PROJECT_ID environment variable required")
        self.client = secretmanager.SecretManagerServiceClient()
    
    def get_secret(self, secret_id: str, version_id: str = "latest") -> str:
        """Retrieve secret value from Secret Manager"""
        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
            response = self.client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_id}: {e}")
            raise

# Core Event Listener
class FirestoreListener:
    """Real-time Firestore listener for event-driven architecture"""
    
    def __init__(self, db):
        self.db = db
        self.callbacks = {}
        self.running = False
        
    def register_callback(self, collection_path: str, callback_func):
        """Register callback for collection changes"""
        self.callbacks[collection_path] = callback_func
    
    async def start_listening(self):
        """Start listening to all registered collections"""
        self.running = True
        logger.info("Starting Firestore real-time listeners")
        
        # Create callbacks for each collection
        for collection_path, callback in self.callbacks.items():
            asyncio.create_task(self._listen_collection(collection_path, callback))
    
    async def _listen_collection(self, collection_path: str, callback):
        """Internal method to listen to collection changes"""
        try:
            # Query with real-time updates
            query_ref = self.db.collection(collection_path).order_by('timestamp', direction=firestore.Query.DESCENDING).limit(10)
            
            # Watch the query
            query_watch = query_ref.on_snapshot(
                lambda doc_snapshot, changes, read_time: self._on_snapshot(doc_snapshot, changes, read_time, callback)
            )
            
            logger.info(f"Listening to {collection_path}")
            
            # Keep alive
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Error in listener for {collection_path}: {e}")
            # Exponential backoff and retry
            await asyncio.sleep(5)
            if self.running:
                asyncio.create_task(self._listen_collection(collection_path, callback))
    
    def _on_snapshot(self, doc_snapshot, changes, read_time, callback):
        """Handle snapshot changes"""
        try:
            for change in changes:
                if change.type.name == 'ADDED':
                    doc_data = change.document.to_dict()
                    doc_data['_id'] = change.document.id
                    
                    # Execute callback in separate thread
                    asyncio.create_task(self._execute_callback(callback, doc_data))
        except Exception as e:
            logger.error(f"Error processing snapshot: {e}")
    
    async def _execute_callback(self, callback, data):
        """Execute callback with error handling"""
        try:
            await callback(data)
        except Exception as e:
            logger.error(f"Callback execution failed: {e}")
    
    def stop(self):
        """Stop all listeners"""
        self.running = False
        logger.info("Firestore listeners stopped")

# Circuit Breaker Pattern
class CircuitBreaker:
    """Circuit breaker for system protection"""
    
    def __init__(self, failure_threshold: int = 3, reset_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        
    def record_failure(self):
        """Record a failure and update state"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"Circuit breaker OPENED after {self.failure_count} failures")
    
    def record_success(self):
        """Record success and reset if needed"""
        self.failure_count = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info("Circuit breaker CLOSED after successful operation")
    
    def can_execute(self) -> bool:
        """Check if operation is allowed"""
        if self.state == "CLOSED":
            return True
        elif self.state == "OPEN":
            # Check if reset timeout has passed
            if self.last_failure_time:
                time_since_failure = (datetime.now() - self.last_failure_time).total_seconds()
                if time_since_failure > self.reset_timeout:
                    self.state = "HALF_OPEN"
                    logger.info("Circuit breaker HALF_OPEN for trial")
                    return True
            return False
        elif self.state == "HALF_OPEN":
            return True  # Allow one trial operation
        
        return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status"""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None
        }

# Main entry point for Cloud Functions
db = initialize_firebase()
listener = FirestoreListener(db)

# Global circuit breakers
opportunity_circuit_breaker = CircuitBreaker(failure_threshold=5, reset_timeout=300)
execution_circuit_breaker = CircuitBreaker(failure_threshold=3, reset_timeout=600)

logger.info("Project Nexus Core Orchestrator initialized")