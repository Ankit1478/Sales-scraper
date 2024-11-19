import json
import logging
import os
import time
from typing import Dict, List, Optional, Union
from fastapi.middleware.cors import CORSMiddleware
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException,Depends
from pydantic import BaseModel
import firebase_admin
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import db,auth
from firebase_admin import credentials, firestore
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

security = HTTPBearer()

cred = credentials.Certificate('./services.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SalesNavQueryData(BaseModel):
    """Request model for Sales Navigator search"""
    url: str
    cookie: str
    userId: str

class LinkedInProfile(BaseModel):
    """Model for LinkedIn profile data"""
    firstName: str
    lastName: str
    title: str
    companyName: str
    industry: Optional[str]
    companyLocation: Optional[str]
    profileLocation: Optional[str]
    connectionDegree: Optional[str]
    profileImageUrl: Optional[str]
    sharedConnectionsCount: int = 0
    defaultProfileUrl: Optional[str]
    companyUrl: Optional[str]

class PhantomBusterClient:
    """Client for interacting with PhantomBuster API"""
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("PHANTOM_KEY")
        if not self.api_key:
            raise ValueError("PhantomBuster API key not found in environment variables")
        
        self.headers = {
            "Content-Type": "application/json",
            "X-Phantombuster-Key-1": self.api_key
        }
        self.phantom_ids = ["2696753432671290", "2786592566218024", "8109199544661391"]
        self.base_url = "https://api.phantombuster.com/api/v2"
        self.max_retries = 5
        self.retry_delay = 60 

    def wait_for_phantom_availability(self, phantom_id: str) -> bool:
        """Wait for a phantom to become available with retries"""
        for attempt in range(self.max_retries):
            try:
                if not self.is_phantom_running(phantom_id):
                    return True
                
                logger.info(f"Phantom {phantom_id} is busy. Attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                
            except HTTPException as e:
                logger.error(f"Error checking phantom status: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                
        return False

    def is_phantom_running(self, phantom_id: str) -> bool:
        """Check if a phantom is currently running"""
        url = f"{self.base_url}/agents/fetch?id={phantom_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=f"Error checking phantom status: {response.text}"
            )
        return response.json().get("status") == "RUNNING"

    def get_available_phantom(self) -> str:
        """Get first available phantom ID with retry logic"""
        for phantom_id in self.phantom_ids:
            if self.wait_for_phantom_availability(phantom_id):
                return phantom_id
                
        raise HTTPException(
            status_code=503,
            detail="All phantoms are busy and maximum retry attempts reached"
        )

    def trigger_phantom(self, phantom_id: str, linkedin_cookies: str, search_url: str) -> str:
        """Trigger phantom with retry logic for concurrent execution limits"""
        url = f"{self.base_url}/agents/launch"
        payload = {
            "id": phantom_id,
            "argument": {
                "searches": search_url,
                "sessionCookie": linkedin_cookies
            }
        }

        for attempt in range(self.max_retries):
            try:
                response = requests.post(url, json=payload, headers=self.headers)
                
                if response.status_code == 200:
                    return response.json()["containerId"]
                
                error_data = response.json()
                if "maxParallelismReached" in str(error_data):
                    logger.warning(f"Maximum parallelism reached. Attempt {attempt + 1}/{self.max_retries}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                
                raise HTTPException(
                    status_code=500,
                    detail=f"Error launching phantom: {response.text}"
                )
                
            except requests.exceptions.RequestException as e:
                if attempt == self.max_retries - 1:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Network error launching phantom: {str(e)}"
                    )
                time.sleep(self.retry_delay)

        raise HTTPException(
            status_code=503,
            detail="Maximum retry attempts reached while launching phantom"
        )

    def check_container_status(self, container_id: str) -> None:
        """Check container status until finished with improved error handling"""
        url = f"{self.base_url}/containers/fetch?id={container_id}"
        max_retries = 30
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = requests.get(url, headers=self.headers)
                if response.status_code != 200:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to check container status: {response.text}"
                    )
                    
                status = response.json().get("status")
                if status == "finished":
                    return
                elif status == "failed":
                    error_message = response.json().get("error", "Unknown error")
                    raise HTTPException(
                        status_code=500,
                        detail=f"Container execution failed: {error_message}"
                    )
                    
                retry_count += 1
                time.sleep(10)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error checking container status: {str(e)}")
                retry_count += 1
                time.sleep(10)
            
        raise HTTPException(
            status_code=504,
            detail="Container execution timed out"
        )

# Rest of the code remains the same...
class LinkedInDataProcessor:
    """Process and clean LinkedIn data"""
    @staticmethod
    def process_profile(item: Dict) -> Dict:
        """Process individual profile data"""
        return {
            "firstName": item.get("firstName", ""),
            "lastName": item.get("lastName", ""),
            "title": item.get("title", ""),
            "companyName": item.get("companyName", ""),
            "industry": item.get("industry", ""),
            "companyLocation": item.get("companyLocation", ""),
            "profileLocation": item.get("location", ""),
            "connectionDegree": item.get("connectionDegree", ""),
            "profileImageUrl": item.get("profileImageUrl", ""),
            "sharedConnectionsCount": item.get("sharedConnectionsCount", 0),
            "defaultProfileUrl": item.get("defaultProfileUrl", ""),
            "companyUrl": item.get("CompanyUrl", "") or item.get("regularCompanyUrl", "")
        }

    @staticmethod
    def fetch_and_clean_data(container_id: str, phantom_client: PhantomBusterClient) -> List[Dict]:
        """Fetch and clean data from container"""
        url = f"{phantom_client.base_url}/containers/fetch-result-object?id={container_id}"
        response = requests.get(url, headers=phantom_client.headers)
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch data from PhantomBuster. Status code: {response.status_code}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to fetch data: {response.text}"
            )

        try:
            # Log the raw response for debugging
            logger.info(f"Raw response from PhantomBuster: {response.text}")
            
            response_json = response.json()
            logger.info(f"Parsed JSON response: {response_json}")
            
            result = response_json.get("resultObject")
            if result is None:
                logger.error("No resultObject found in PhantomBuster response")
                raise HTTPException(
                    status_code=500,
                    detail="No data received from PhantomBuster"
                )
            
            # If result is a string, try to parse it as JSON
            if isinstance(result, str):
                try:
                    logger.info("Attempting to parse string result as JSON")
                    data = json.loads(result)
                    logger.info(f"Successfully parsed string result: {type(data)}")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse result string as JSON: {str(e)}")
                    raise HTTPException(
                        status_code=500,
                        detail="Invalid data format received from PhantomBuster"
                    )
            else:
                data = result

            # Handle case where data is in external URL
            if isinstance(data, dict) and "jsonUrl" in data:
                logger.info(f"Found external JSON URL: {data['jsonUrl']}")
                json_response = requests.get(data["jsonUrl"])
                if json_response.status_code != 200:
                    logger.error(f"Failed to fetch JSON from external URL. Status code: {json_response.status_code}")
                    raise HTTPException(
                        status_code=500,
                        detail="Failed to fetch JSON from external URL"
                    )
                data = json_response.json()
                logger.info("Successfully fetched data from external URL")

            # Handle empty data cases
            if data is None:
                logger.info("No data found in PhantomBuster response")
                return []  # Return empty list instead of raising an error
                
            # Convert single item to list if necessary
            if isinstance(data, dict):
                logger.info("Converting single item dict to list")
                data = [data]

            # Ensure data is a list
            if not isinstance(data, list):
                logger.error(f"Unexpected data format: {type(data)}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Unexpected data format received from PhantomBuster: {type(data)}"
                )

            logger.info(f"Processing {len(data)} profiles")
            return [LinkedInDataProcessor.process_profile(item) for item in data]

        except Exception as e:
            logger.error(f"Error processing data: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Error processing data: {str(e)}"
            )         
   
   
   
def check_user_exists(user_id: str) -> bool:
    """Check if user exists in 'users' collection"""
    try:
        doc_ref = db.collection('users').document(str(user_id))
        return doc_ref.get().exists
    except Exception as e:
        logger.error(f"Error checking user existence: {str(e)}")
        return False

def create_new_user(user_id: str, data: dict):
    """Create a new user document in 'users' collection"""
    try:
        doc_ref = db.collection('users').document(str(user_id))
        initial_data = {
            "Events_URL": data.get("Events_URL", ""),
            "sessionCookie": data.get("sessionCookie", ""),
        }
        doc_ref.set(initial_data)
        logger.info(f"Successfully created new user: {user_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to create user {user_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create new user")

def updateUser(collection_name: str, document_id: str, field_name: str, field_value: str):
    """Update user field if exists, create if not exists"""
    try:
        user_id = str(document_id)
        user_exists = check_user_exists(user_id)
        
        data = {
            field_name: str(field_value),
            "last_updated": firestore.SERVER_TIMESTAMP
        }

        if not user_exists:
            logger.info(f"User {user_id} not found. Creating new user...")
            return create_new_user(user_id, data)
        else:
            doc_ref = db.collection(collection_name).document(user_id)
            doc_ref.update(data)
            logger.info(f"Updated existing user: {user_id}")
            return True

    except Exception as e:
        logger.error(f"Error in updateUser: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to process user data")        
    
async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    try:
        decoded_token = auth.verify_id_token(token)
        return decoded_token
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")

def is_user_authorized(user_id: str) -> bool:
    return True  

    
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/scrape")
async def search_sales_navigator(query_data: SalesNavQueryData,user: dict = Depends(verify_token)):
    """
    Endpoint to search Sales Navigator and process results
    """
    print("hii")
    try:
        if not user or 'uid' not in user:
            raise HTTPException(status_code=400, detail="Invalid user data")
        phantom_client = PhantomBusterClient()
        
        user_id = str(user['uid'])
        logger.info(f"Processing request for user: {user_id}")
        user_doc_ref = db.collection('users').document(user_id)
        
        # Get available phantom and trigger search
        phantom_id = phantom_client.get_available_phantom()
        container_id = phantom_client.trigger_phantom(
            phantom_id,
            query_data.cookie,
            query_data.url
        )
        
        # Wait for completion
        phantom_client.check_container_status(container_id)
        
        # Process results
        results = LinkedInDataProcessor.fetch_and_clean_data(container_id, phantom_client)
        
        try:
            user_data = {
                "Events_URL": query_data.url,
                "sessionCookie": query_data.cookie,
            }
            
            # Get user document
            user_doc = user_doc_ref.get()
            
            if not user_doc.exists:
                user_doc_ref.set(user_data)
                logger.info(f"Created new user document for {user_id}")
            else:
                user_doc_ref.update(user_data)
                logger.info(f"Updated existing user {user_id}")

        except Exception as e:
            logger.error(f"Error handling user document: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Failed to handle user document: {str(e)}")

        return {
            "events": results,
            "userId": user_id,
            "message":  query_data.url,
        }
        
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )