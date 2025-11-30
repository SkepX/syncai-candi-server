"""
Masumi-Compatible CrewAI Agent Server
Implements MIP-003 standard for decentralized AI agent payments
"""

import os
import sys
import uuid
import time
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import uvicorn
import requests

# Import your CrewAI agent
from crew_agent import process_query, get_agent

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

class Config:
    """Application configuration from environment variables"""

    # Payment Service Configuration
    PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:3001/api/v1")
    PAYMENT_API_KEY = os.getenv("PAYMENT_API_KEY", "")

    # Agent Configuration
    AGENT_IDENTIFIER = os.getenv("AGENT_IDENTIFIER", "")
    PAYMENT_AMOUNT = int(os.getenv("PAYMENT_AMOUNT", "2000000"))  # 10 ADA in lovelace
    PAYMENT_UNIT = os.getenv("PAYMENT_UNIT", "lovelace")
    SELLER_VKEY = os.getenv("SELLER_VKEY", "")
    SMART_CONTRACT_ADDRESS = os.getenv("SMART_CONTRACT_ADDRESS", "")

    # Network Configuration
    NETWORK = os.getenv("NETWORK", "Preprod")
    PORT = int(os.getenv("PORT", "8000"))
    HOST = os.getenv("HOST", "0.0.0.0")

    # OpenAI/Anthropic API
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")


config = Config()


# ============================================================================
# Data Models (MIP-003 Compliant)
# ============================================================================

class JobStatus(str, Enum):
    """Job execution states"""
    PENDING_PAYMENT = "pending_payment"
    PAYMENT_CONFIRMED = "payment_confirmed"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class InputType(str, Enum):
    """Input data types"""
    TEXT = "text"
    FILE = "file"
    JSON = "json"


class InputField(BaseModel):
    """Schema for a single input field"""
    name: str = Field(..., description="Field identifier")
    type: InputType = Field(..., description="Data type of the field")
    description: str = Field(..., description="Human-readable description")
    required: bool = Field(default=True, description="Whether field is required")
    default: Optional[Any] = Field(default=None, description="Default value")


class InputSchema(BaseModel):
    """Agent input schema definition"""
    fields: List[InputField] = Field(..., description="Input fields required")
    examples: Optional[List[Dict[str, Any]]] = Field(default=None)


class StartJobRequest(BaseModel):
    """
    Request to start a new agent job

    Example:
        {
            "identifier_from_purchaser": "a1b2c3d4e5f6a7b8c9d0e1f2",
            "input_data": {
                "query": "What are the top 5 DReps by voting power on Cardano?"
            }
        }
    """
    identifier_from_purchaser: str = Field(
        ...,
        description="Unique hex identifier (14-26 characters, must be even length). Example: 'a1b2c3d4e5f6a7b8c9d0e1f2'",
        min_length=14,
        max_length=26,
        pattern="^[0-9a-fA-F]+$",
        example="a1b2c3d4e5f6a7b8c9d0e1f2"
    )
    input_data: Any = Field(
        ...,
        description="Your question or query for the Cardano blockchain agent",
        example={"query": "What are the top 5 DReps by voting power on Cardano?"}
    )


class StartJobResponse(BaseModel):
    """Response when starting a job"""
    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(..., description="Current job status")
    payment_request: Dict[str, Any] = Field(..., description="Payment details")
    message: str = Field(..., description="Human-readable message")


class JobStatusResponse(BaseModel):
    """Job status check response"""
    job_id: str
    status: JobStatus
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: str
    updated_at: str
    payment_confirmed: bool = False
    result_submitted: Optional[bool] = None
    result_submitted_at: Optional[str] = None


class AvailabilityResponse(BaseModel):
    """Service availability response"""
    available: bool = Field(..., description="Whether service is operational")
    message: str = Field(..., description="Status message")
    agent_info: Optional[Dict[str, Any]] = Field(default=None)


# ============================================================================
# In-Memory Storage (Replace with Database in Production)
# ============================================================================

class JobStore:
    """In-memory job storage - USE DATABASE IN PRODUCTION"""

    def __init__(self):
        self.jobs: Dict[str, Dict[str, Any]] = {}

    def create_job(self, job_id: str, job_data: Dict[str, Any]) -> None:
        """Store a new job"""
        self.jobs[job_id] = {
            **job_data,
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat()
        }
        logger.info(f"Created job {job_id}")

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a job"""
        return self.jobs.get(job_id)

    def update_job(self, job_id: str, updates: Dict[str, Any]) -> None:
        """Update job data"""
        if job_id in self.jobs:
            self.jobs[job_id].update(updates)
            self.jobs[job_id]["updated_at"] = datetime.utcnow().isoformat()
            logger.info(f"Updated job {job_id}: {updates}")

    def delete_job(self, job_id: str) -> None:
        """Delete a job"""
        if job_id in self.jobs:
            del self.jobs[job_id]
            logger.info(f"Deleted job {job_id}")


job_store = JobStore()


# ============================================================================
# Payment Service Integration
# ============================================================================

class PaymentService:
    """Integration with Masumi Payment Service"""

    def __init__(self):
        self.base_url = config.PAYMENT_SERVICE_URL
        self.api_key = config.PAYMENT_API_KEY

    def check_payment(self, blockchain_identifier: str) -> Dict[str, Any]:
        """
        Check payment status with Masumi Payment Service

        Args:
            blockchain_identifier: Blockchain identifier from payment creation

        Returns:
            Payment status information
        """
        try:
            # Use the payment endpoint (not purchase) to check status
            url = f"{self.base_url}/payment/resolve-blockchain-identifier"
            headers = {
                "token": self.api_key,
                "accept": "application/json",
                "Content-Type": "application/json"
            }
            payload = {
                "blockchainIdentifier": blockchain_identifier,
                "network": config.NETWORK,
                "includeHistory": "false"
            }

            logger.info(f"Checking payment status: {url}")
            response = requests.post(url, headers=headers, json=payload, timeout=10)

            if response.status_code == 200:
                data = response.json()
                logger.info(f"Payment check response status: {data.get('status')}")
                logger.info(f"Payment NextAction: {data.get('data', {}).get('NextAction', {}).get('requestedAction')}")
                return data
            else:
                logger.warning(f"Payment check failed: {response.status_code} - {response.text}")
                return {"status": "error", "message": response.text}

        except Exception as e:
            logger.error(f"Error checking payment: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    def create_payment(self, agent_identifier: str, identifier_from_purchaser: str, input_data: Any) -> Dict[str, Any]:
        """
        Create a payment request via Masumi Payment Service

        Args:
            agent_identifier: Agent's unique identifier
            identifier_from_purchaser: Purchaser's hex identifier
            input_data: Job input data

        Returns:
            Payment creation response with blockchain_identifier
        """
        try:
            import hashlib
            import json
            from datetime import datetime, timedelta

            # Calculate input hash
            input_str = json.dumps(input_data, sort_keys=True)
            input_hash = hashlib.sha256(input_str.encode()).hexdigest()

            # Set payment times (ISO 8601 format with Z suffix)
            now = datetime.utcnow()
            pay_by_time = (now + timedelta(minutes=10)).isoformat() + "Z"
            submit_result_time = (now + timedelta(minutes=20)).isoformat() + "Z"

            url = f"{self.base_url}/payment"
            headers = {
                "token": self.api_key,
                "Content-Type": "application/json"
            }

            payload = {
                "agentIdentifier": agent_identifier,
                "network": config.NETWORK,
                "inputHash": input_hash,
                "payByTime": pay_by_time,
                "submitResultTime": submit_result_time,
                "identifierFromPurchaser": identifier_from_purchaser
            }

            logger.info(f"Creating payment with Masumi: {url}")
            logger.info(f"Payment payload: {json.dumps(payload, indent=2)}")
            response = requests.post(url, headers=headers, json=payload, timeout=10)

            if response.status_code == 200 or response.status_code == 201:
                data = response.json()
                logger.info(f"Payment created successfully: {json.dumps(data, indent=2)}")
                return data
            else:
                logger.error(f"Payment creation failed: {response.status_code} - {response.text}")
                return {"status": "error", "message": response.text}

        except Exception as e:
            logger.error(f"Error creating payment: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    def is_payment_confirmed(self, blockchain_identifier: str) -> bool:
        """
        Check if payment is confirmed on blockchain

        Args:
            blockchain_identifier: Blockchain identifier from payment creation

        Returns:
            True if payment is confirmed (funds received on-chain)
        """
        result = self.check_payment(blockchain_identifier)

        if result.get("status") == "success":
            data = result.get("data", {})

            # Check if buyer wallet is set (means payment received)
            buyer_wallet = data.get("BuyerWallet")
            if buyer_wallet:
                logger.info(f"✅ Payment confirmed - BuyerWallet found: {buyer_wallet.get('id')}")
                return True

            # Check if transaction is on-chain
            on_chain_state = data.get("onChainState")
            if on_chain_state:
                logger.info(f"✅ Payment confirmed with onChainState: {on_chain_state}")
                return True

            # Check NextAction state
            next_action = data.get("NextAction", {})
            requested_action = next_action.get("requestedAction", "")

            # Payment states progression:
            # WaitingForExternalAction (no payment) -> Payment received -> FundsLockingRequested -> FundsLocked
            if requested_action in ["FundsLockingRequested", "FundsLocked", "ResultSubmitted", "Completed", "DisputeResolved"]:
                logger.info(f"✅ Payment confirmed with action: {requested_action}")
                return True

            # Check CurrentTransaction status
            current_transaction = data.get("CurrentTransaction")
            if current_transaction:
                tx_status = current_transaction.get("status", "").lower()
                if tx_status in ["confirmed", "succeeded", "completed"]:
                    logger.info(f"✅ Payment confirmed with transaction status: {tx_status}")
                    return True

            # Still waiting for payment
            logger.info(f"⏳ Payment pending. Current action: {requested_action}")

        logger.debug(f"Payment check result: {result}")
        return False

    def submit_result(self, blockchain_identifier: str, result_data: Any) -> Dict[str, Any]:
        """
        Submit result hash to Masumi Payment Service after job completion

        Args:
            blockchain_identifier: Blockchain identifier from payment creation
            result_data: The result data to submit

        Returns:
            Submission response from Masumi
        """
        try:
            import hashlib
            import json

            # Calculate result hash
            result_str = json.dumps(result_data, sort_keys=True)
            result_hash = hashlib.sha256(result_str.encode()).hexdigest()

            url = f"{self.base_url}/payment/submit-result"
            headers = {
                "token": self.api_key,
                "Content-Type": "application/json"
            }

            payload = {
                "network": config.NETWORK,
                "blockchainIdentifier": blockchain_identifier,
                "submitResultHash": result_hash
            }

            logger.info(f"Submitting result to Masumi: {url}")
            logger.info(f"Result hash: {result_hash}")
            response = requests.post(url, headers=headers, json=payload, timeout=10)

            if response.status_code == 200 or response.status_code == 201:
                data = response.json()
                logger.info(f"✅ Result submitted successfully: {json.dumps(data, indent=2)}")
                return data
            else:
                logger.error(f"Result submission failed: {response.status_code} - {response.text}")
                return {"status": "error", "message": response.text}

        except Exception as e:
            logger.error(f"Error submitting result: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}


payment_service = PaymentService()


# ============================================================================
# Background Job Processing
# ============================================================================

def process_job_background(job_id: str):
    """
    Process job in background after payment confirmation

    Args:
        job_id: Job identifier
    """
    try:
        job = job_store.get_job(job_id)
        if not job:
            logger.error(f"Job {job_id} not found")
            return

        # Update status to processing
        job_store.update_job(job_id, {"status": JobStatus.PROCESSING})

        # Extract input data - handle both dict and array formats
        input_data = job.get("input_data", {})

        # Convert array format to dict if needed
        if isinstance(input_data, list):
            # Masumi format: [{"key": "query", "value": "question"}]
            input_dict = {item["key"]: item["value"] for item in input_data}
        else:
            # Direct dict format: {"query": "question"}
            input_dict = input_data

        query = input_dict.get("query") or input_dict.get("text", "")

        if not query:
            raise ValueError("No query provided in input_data")

        logger.info(f"Processing job {job_id} with query: {query}")

        # Call your CrewAI agent
        result = process_query(query)

        if result.get("success"):
            # Job completed successfully
            job_store.update_job(job_id, {
                "status": JobStatus.COMPLETED,
                "result": result.get("result"),
                "completed_at": datetime.utcnow().isoformat()
            })
            logger.info(f"Job {job_id} completed successfully")

            # Submit result to Masumi to release payment
            blockchain_identifier = job.get("blockchain_identifier")
            if blockchain_identifier:
                logger.info(f"Submitting result to Masumi for job {job_id}")
                submission_result = payment_service.submit_result(
                    blockchain_identifier=blockchain_identifier,
                    result_data=result.get("result")
                )

                if submission_result.get("status") == "success":
                    logger.info(f"✅ Result submitted to Masumi - funds will be released to seller")
                    job_store.update_job(job_id, {
                        "result_submitted": True,
                        "result_submitted_at": datetime.utcnow().isoformat()
                    })
                else:
                    logger.error(f"❌ Failed to submit result to Masumi: {submission_result.get('message')}")
                    job_store.update_job(job_id, {
                        "result_submitted": False,
                        "submission_error": submission_result.get("message")
                    })
            else:
                logger.warning(f"No blockchain_identifier found for job {job_id} - cannot submit result")
        else:
            # Job failed
            job_store.update_job(job_id, {
                "status": JobStatus.FAILED,
                "error": result.get("error", "Unknown error"),
                "failed_at": datetime.utcnow().isoformat()
            })
            logger.error(f"Job {job_id} failed: {result.get('error')}")

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}", exc_info=True)
        job_store.update_job(job_id, {
            "status": JobStatus.FAILED,
            "error": str(e),
            "failed_at": datetime.utcnow().isoformat()
        })


def check_payment_and_process(job_id: str):
    """
    Check payment status and start processing if confirmed

    Args:
        job_id: Job identifier
    """
    try:
        job = job_store.get_job(job_id)
        if not job:
            return

        blockchain_identifier = job.get("blockchain_identifier")

        if not blockchain_identifier:
            logger.error(f"No blockchain_identifier found for job {job_id}")
            return

        # Check payment status using blockchain identifier
        if payment_service.is_payment_confirmed(blockchain_identifier):
            logger.info(f"Payment confirmed for job {job_id}")
            job_store.update_job(job_id, {
                "status": JobStatus.PAYMENT_CONFIRMED,
                "payment_confirmed": True,
                "payment_confirmed_at": datetime.utcnow().isoformat()
            })

            # Start processing
            process_job_background(job_id)
        else:
            logger.info(f"Payment not yet confirmed for job {job_id}")

    except Exception as e:
        logger.error(f"Error checking payment for job {job_id}: {e}", exc_info=True)


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="CANDI - Cardano Agent for Network Data Intelligence",
    description="""
# CANDI - Cardano Agent for Network Data Intelligence

**CANDI** is an AI agent specialized in Cardano blockchain data analysis with **86+ tools** covering governance, DReps, tokens, NFTs, DeFi protocols, and on-chain analytics.

Powered by Claude Sonnet 4 and uses decentralized payments via Masumi Payment Service (MIP-003 standard).

## 🎯 Capabilities
- 🏛️ **Governance Analysis**: DReps, SPOs, voting power, proposals
- 💰 **Token Economics**: Prices, market caps, token analytics
- 🎨 **NFT Market Data**: Collections, volumes, floor prices, trends
- 🏦 **DeFi Analytics**: TVL, protocols, liquidity pools
- 📊 **On-Chain Data**: Transactions, addresses, staking metrics
- 🔍 **Network Intelligence**: Real-time blockchain insights

## 🚀 Quick Start
1. **POST /start_job** - Submit your query with a hex identifier
2. **Pay** - Send 2 ADA to the payment address you receive
3. **GET /status** - Check results (auto-processes after payment)

## 💳 Payment
- **Amount**: 2 ADA (2,000,000 lovelace)
- **Network**: Cardano Mainnet
- **Method**: Blockchain smart contract escrow
- **Standard**: MIP-003 compliant

## 📝 Example Query
```json
{
  "identifier_from_purchaser": "a1b2c3d4e5f6a7b8c9d0e1f2",
  "input_data": {
    "query": "What are the top 5 DReps by voting power on Cardano?"
  }
}
```

## 📚 Documentation
- **/input_schema** - See supported query formats
- **/availability** - Check service status
- **/health** - System health check

## 👥 Team
**SyncAI** | Contact: adnan@syncai.pro | [SyncGovHub.com](https://syncgovhub.com/)
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "MIP-003 Standard",
            "description": "Masumi payment protocol endpoints for decentralized AI agent payments"
        },
        {
            "name": "Information",
            "description": "Agent capabilities, schema, and health checks"
        }
    ]
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# MIP-003 Standard Endpoints
# ============================================================================

@app.get("/input_schema", response_model=InputSchema, tags=["Information"])
async def get_input_schema():
    """
    Get input schema - See what queries this agent accepts

    Returns the data format expected by this AI agent. Use this to understand
    what kind of questions you can ask and how to format them.
    """
    return InputSchema(
        fields=[
            InputField(
                name="query",
                type=InputType.TEXT,
                description="Natural language question about Cardano blockchain (governance, tokens, NFTs, DeFi, etc.)",
                required=True
            ),
            InputField(
                name="text",
                type=InputType.TEXT,
                description="Alternative field name for query",
                required=False
            )
        ],
        examples=[
            {"query": "What are the top 5 DReps by voting power?"},
            {"query": "Show me recent governance proposals on Cardano"},
            {"text": "What is the current ADA price and market cap?"}
        ]
    )


@app.get("/availability", response_model=AvailabilityResponse, tags=["Information"])
async def check_availability():
    """
    Check agent availability - Is the service ready?

    Returns the current operational status of the AI agent and its capabilities.
    Use this before starting a job to ensure the service is available.
    """
    try:
        # Check if CrewAI agent is initialized
        crew = get_agent()
        agent_available = crew.agent is not None

        return AvailabilityResponse(
            available=agent_available,
            message="CANDI is operational and ready" if agent_available else "CANDI service unavailable",
            agent_info={
                "name": "CANDI - Cardano Agent for Network Data Intelligence",
                "version": "1.0.0",
                "capabilities": [
                    "Governance analysis (DReps, SPOs, voting power, proposals)",
                    "Token economics and pricing data",
                    "NFT market data and analytics",
                    "DeFi protocol analysis and TVL tracking",
                    "On-chain data queries and network intelligence"
                ],
                "tools": "86+ Cardano blockchain tools",
                "model": "Claude Sonnet 4",
                "network": config.NETWORK,
                "team": "SyncAI",
                "contact": "adnan@syncai.pro"
            }
        )
    except Exception as e:
        logger.error(f"Availability check failed: {e}")
        return AvailabilityResponse(
            available=False,
            message=f"Service error: {str(e)}"
        )


@app.post("/start_job", response_model=StartJobResponse, tags=["MIP-003 Standard"])
async def start_job(request: StartJobRequest, background_tasks: BackgroundTasks):
    """
    Start a new AI agent job with Cardano blockchain payment (MIP-003)

    This endpoint creates a job and returns payment details. The job will begin processing
    automatically once payment is confirmed on the Cardano blockchain.

    **How to use:**
    1. Send your query with a unique hex identifier
    2. Receive payment address and blockchain identifier
    3. Send 2 ADA to the provided payment address
    4. Agent processes your query automatically after payment confirmation
    5. Check results using GET /status with your job_id

    **Request Body:**
    - `identifier_from_purchaser`: A unique hex string (14-26 chars, even length)
      - Must be hexadecimal (0-9, a-f)
      - Example: "a1b2c3d4e5f6a7b8c9d0e1f2"
      - You can generate one at: https://www.random.org/strings/ (choose hex)

    - `input_data`: Your question about Cardano blockchain
      - Format: {"query": "your question here"}
      - Example queries:
        * "What are the top 5 DReps by voting power?"
        * "Show me recent governance proposals"
        * "What is the current ADA price?"

    **Response:**
    Returns payment details including:
    - `job_id`: Use this to check status later
    - `payment_request.payment_address`: Send 2 ADA here
    - `payment_request.blockchain_identifier`: Unique payment tracking ID

    **Example Request:**
    ```json
    {
      "identifier_from_purchaser": "a1b2c3d4e5f6a7b8c9d0e1f2",
      "input_data": {
        "query": "What are the top 5 DReps by voting power on Cardano?"
      }
    }
    ```

    **Payment Amount:** 2 ADA (2,000,000 lovelace)

    **Network:** Mainnet or Preprod (check /health endpoint)
    """
    try:
        # Get identifier from purchaser
        identifier = request.identifier_from_purchaser

        # Validate identifier length (Masumi requires 14-26 characters)
        if not identifier or len(identifier) < 14 or len(identifier) > 26:
            raise HTTPException(
                status_code=400,
                detail="identifier_from_purchaser must be between 14 and 26 characters (Masumi requirement). Example: a1b2c3d4e5f6a7b8c9d0e1"
            )

        # Validate identifier is hexadecimal (Masumi requirement)
        try:
            int(identifier, 16)  # Check if valid hex
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="identifier_from_purchaser must be a valid hexadecimal string (0-9, a-f). Example: a1b2c3d4e5f6a7b8c9d0e1"
            )

        # Create unique job ID
        job_id = str(uuid.uuid4())

        # Create payment via Masumi Payment Service
        payment_response = payment_service.create_payment(
            agent_identifier=config.AGENT_IDENTIFIER,
            identifier_from_purchaser=identifier,
            input_data=request.input_data
        )

        if payment_response.get("status") == "error":
            raise HTTPException(
                status_code=500,
                detail=f"Payment creation failed: {payment_response.get('message')}"
            )

        # Extract payment data from Masumi response
        payment_data = payment_response.get("data", {})
        blockchain_identifier = payment_data.get("blockchainIdentifier")

        if not blockchain_identifier:
            raise HTTPException(
                status_code=500,
                detail="Failed to get blockchain identifier from payment service"
            )

        # Extract payment source details from Masumi response
        payment_source = payment_data.get("PaymentSource", {})
        smart_contract_address = payment_source.get("smartContractAddress") or config.SMART_CONTRACT_ADDRESS

        # Create payment request info for response (clean, user-friendly format)
        payment_request = {
            "blockchain_identifier": blockchain_identifier,
            "payment_address": smart_contract_address,
            "agent_identifier": config.AGENT_IDENTIFIER,
            "amount": config.PAYMENT_AMOUNT,
            "unit": config.PAYMENT_UNIT,
            "seller_vkey": config.SELLER_VKEY,
            "network": config.NETWORK,
            "payment_service_url": config.PAYMENT_SERVICE_URL,
            "pay_by_time": payment_data.get("payByTime"),
            "instructions": f"Send {config.PAYMENT_AMOUNT} {config.PAYMENT_UNIT} to {smart_contract_address}"
        }

        # Store job with blockchain identifier
        job_store.create_job(job_id, {
            "job_id": job_id,
            "status": JobStatus.PENDING_PAYMENT,
            "identifier_from_purchaser": identifier,
            "blockchain_identifier": blockchain_identifier,
            "input_data": request.input_data,
            "payment_request": payment_request,
            "payment_confirmed": False
        })

        # Schedule payment check in background
        background_tasks.add_task(check_payment_and_process, job_id)

        return StartJobResponse(
            job_id=job_id,
            status=JobStatus.PENDING_PAYMENT,
            payment_request=payment_request,
            message="Job created. Please complete payment to begin processing."
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status", response_model=JobStatusResponse, tags=["MIP-003 Standard"])
async def get_job_status(job_id: str, background_tasks: BackgroundTasks):
    """
    Check the status and results of your job (MIP-003)

    Use the `job_id` you received from POST /start_job to check your job status.

    **Job Status Flow:**
    1. `pending_payment` - Waiting for your payment
    2. `payment_confirmed` - Payment received, processing started
    3. `processing` - AI agent is working on your query
    4. `completed` - Results ready! Check the `result` field
    5. `failed` - Something went wrong, check the `error` field

    **Query Parameters:**
    - `job_id`: The job ID from your /start_job response

    **Response Fields:**
    - `status`: Current job state (see flow above)
    - `result`: Your answer (available when status = "completed")
    - `error`: Error message (if status = "failed")
    - `payment_confirmed`: true if payment received
    - `result_submitted`: true if result sent to payment service (funds released)

    **Example:**
    GET /status?job_id=550e8400-e29b-41d4-a716-446655440000

    **Tip:** Poll this endpoint every 5-10 seconds until status is "completed"
    """
    job = job_store.get_job(job_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # If still pending payment, check again
    if job.get("status") == JobStatus.PENDING_PAYMENT:
        background_tasks.add_task(check_payment_and_process, job_id)

    return JobStatusResponse(
        job_id=job.get("job_id"),
        status=job.get("status"),
        result=job.get("result"),
        error=job.get("error"),
        created_at=job.get("created_at"),
        updated_at=job.get("updated_at"),
        payment_confirmed=job.get("payment_confirmed", False),
        result_submitted=job.get("result_submitted"),
        result_submitted_at=job.get("result_submitted_at")
    )


@app.post("/provide_input", tags=["MIP-003 Standard"])
async def provide_input(job_id: str, additional_input: Dict[str, Any]):
    """
    Provide additional input to a running job (MIP-003)

    **Note:** This agent does not support interactive/multi-turn conversations.
    All queries are processed in a single execution.
    """
    raise HTTPException(
        status_code=501,
        detail="This agent does not support interactive input"
    )


# ============================================================================
# Additional Helper Endpoints
# ============================================================================

@app.get("/", tags=["Information"])
async def root():
    """
    API Information - Quick overview

    Returns basic information about the API, available endpoints, and network configuration.
    """
    return {
        "name": "CANDI",
        "full_name": "Cardano Agent for Network Data Intelligence",
        "version": "1.0.0",
        "description": "AI agent specialized in Cardano blockchain data analysis with 86+ tools",
        "tools": 86,
        "model": "Claude Sonnet 4",
        "team": "SyncAI",
        "contact": "adnan@syncai.pro",
        "website": "https://syncgovhub.com/",
        "masumi_compatible": True,
        "standard": "MIP-003",
        "network": config.NETWORK,
        "payment": {
            "amount": "2 ADA",
            "lovelace": 2000000
        },
        "endpoints": {
            "input_schema": "GET /input_schema",
            "availability": "GET /availability",
            "start_job": "POST /start_job",
            "status": "GET /status?job_id=<id>",
            "docs": "GET /docs",
            "health": "GET /health"
        }
    }


@app.get("/health", tags=["Information"])
async def health_check():
    """
    Health check - System status

    Returns the operational health of the service and whether the AI agent is initialized.
    """
    try:
        crew = get_agent()
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "agent_initialized": crew.agent is not None
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }


# ============================================================================
# Startup/Shutdown Events
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize agent on startup"""
    logger.info("="*80)
    logger.info("CANDI - Cardano Agent for Network Data Intelligence")
    logger.info("="*80)
    logger.info(f"Version: 1.0.0")
    logger.info(f"Model: Claude Sonnet 4")
    logger.info(f"Tools: 86+ Cardano blockchain tools")
    logger.info(f"Network: {config.NETWORK}")
    logger.info(f"Payment: 2 ADA via Masumi (MIP-003)")
    logger.info(f"Team: SyncAI | adnan@syncai.pro")
    logger.info("="*80)

    try:
        crew = get_agent()
        logger.info(f"✅ CANDI initialized: {crew.agent.role}")
    except Exception as e:
        logger.error(f"❌ CANDI initialization failed: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down CANDI...")


# ============================================================================
# Standalone Mode (for testing without payments)
# ============================================================================

def run_standalone():
    """Run agent in standalone mode for testing"""
    logger.info("="*80)
    logger.info("Running in STANDALONE mode (no payment required)")
    logger.info("="*80)

    # Test query
    test_query = "What are the top 5 DReps by voting power on Cardano?"
    logger.info(f"Test query: {test_query}")

    result = process_query(test_query)

    if result.get("success"):
        print("\n" + "="*80)
        print("RESULT:")
        print("="*80)
        print(result.get("result"))
        print("="*80)
    else:
        print(f"\nError: {result.get('error')}")


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    # Check if running in API mode or standalone mode
    if len(sys.argv) > 1 and sys.argv[1] == "api":
        # API mode - start FastAPI server
        logger.info("Starting CANDI in API mode with Masumi payment integration...")
        uvicorn.run(
            app,
            host=config.HOST,
            port=config.PORT,
            log_level="info",
            access_log=True
        )
    else:
        # Standalone mode - run without API/payments
        run_standalone()
