"""
FastAPI Server for Cardano CrewAI MCP Client
Exposes REST API endpoints on port 4326 for querying Cardano blockchain data
"""

import json
import logging
import os
from typing import Dict, Any, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv

# Import CrewAI agent
from crew_agent import process_query, get_agent

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Cardano CrewAI MCP Client",
    description="FastAPI server that uses CrewAI with 86 MCP tools to analyze Cardano blockchain data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response Models
class QueryRequest(BaseModel):
    """Request model for query endpoint"""
    query: str = Field(
        ...,
        description="Natural language query about Cardano blockchain",
        min_length=1,
        example="top 5 dreps by voting power?"
    )
    include_metadata: bool = Field(
        default=True,
        description="Include metadata in response"
    )


class QueryResponse(BaseModel):
    """Response model for query endpoint"""
    success: bool
    result: Optional[str] = None
    query: str
    error: Optional[str] = None
    tools_available: int
    timestamp: str
    processing_time_ms: Optional[float] = None


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str
    timestamp: str
    tools_loaded: int
    crew_initialized: bool


class ToolInfo(BaseModel):
    """Information about a single tool"""
    name: str
    description: str


class ToolsResponse(BaseModel):
    """Response model for tools list endpoint"""
    total_tools: int
    tools: list[ToolInfo]


# Endpoints

@app.get("/", response_model=Dict[str, str])
async def root():
    """Root endpoint with API information"""
    return {
        "name": "Cardano CrewAI MCP Client",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "GET /health",
            "tools": "GET /tools",
            "query_get": "GET /query?q=<your_query>",
            "query_post": "POST /query",
            "docs": "GET /docs"
        }
    }


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    Returns server status and API availability.
    """
    try:
        crew = get_agent()
        crew_initialized = crew.agent is not None
    except Exception as e:
        logger.error(f"Error checking crew status: {e}")
        crew_initialized = False

    return HealthResponse(
        status="healthy" if crew_initialized else "degraded",
        timestamp=datetime.utcnow().isoformat(),
        tools_loaded=1,
        crew_initialized=crew_initialized
    )


@app.get("/tools", response_model=ToolsResponse)
async def list_tools():
    """
    List all available MCP tools.
    Returns empty list since we're using direct API calls.
    """
    return ToolsResponse(
        total_tools=0,
        tools=[]
    )


@app.post("/query", response_model=QueryResponse)
async def query_post(request: QueryRequest):
    """
    Process a Cardano blockchain query using CrewAI Agent (POST method).

    Accepts a natural language query and uses the Cardano Crew Agent
    with MCP tools to provide comprehensive analysis.

    Example request:
    ```json
    {
        "query": "top 5 dreps by voting power on Cardano?",
        "include_metadata": true
    }
    ```
    """
    start_time = datetime.utcnow()
    logger.info(f"Processing POST query with CrewAI: {request.query}")

    try:
        # Process the query using CrewAI agent
        result = process_query(request.query)

        # Calculate processing time
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds() * 1000

        response = QueryResponse(
            success=result.get("success", False),
            result=result.get("result"),
            query=result.get("query", request.query),
            error=result.get("error"),
            tools_available=0,
            timestamp=end_time.isoformat(),
            processing_time_ms=round(processing_time, 2)
        )

        logger.info(f"Query processed successfully in {processing_time:.2f}ms")
        return response

    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error processing query: {str(e)}"
        )


@app.get("/query", response_model=QueryResponse)
async def query_get(q: str):
    """
    Process a Cardano blockchain query using CrewAI Agent (GET method).

    Accepts a natural language query as a URL parameter and uses the Cardano Crew Agent
    with MCP tools to provide comprehensive analysis.

    Example: GET /query?q=What are the top tokens by market cap?
    """
    start_time = datetime.utcnow()
    logger.info(f"Processing GET query with CrewAI: {q}")

    try:
        # Process the query using CrewAI agent
        result = process_query(q)

        # Calculate processing time
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds() * 1000

        response = QueryResponse(
            success=result.get("success", False),
            result=result.get("result"),
            query=result.get("query", q),
            error=result.get("error"),
            tools_available=0,
            timestamp=end_time.isoformat(),
            processing_time_ms=round(processing_time, 2)
        )

        logger.info(f"Query processed successfully in {processing_time:.2f}ms")
        return response

    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error processing query: {str(e)}"
        )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors"""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": str(exc),
            "timestamp": datetime.utcnow().isoformat()
        }
    )


# Startup/Shutdown Events

@app.on_event("startup")
async def startup_event():
    """Initialize CrewAI agent on startup"""
    logger.info("Starting Cardano CrewAI API Server...")

    try:
        crew = get_agent()
        logger.info("Cardano Expert Crew initialized successfully")
        logger.info(f"Agent: {crew.agent.role}")
        logger.info(f"Tools: {len(crew.agent.tools)} tool(s) available")
        logger.info("Server ready on http://localhost:4326")
    except Exception as e:
        logger.error(f"Error initializing crew: {e}", exc_info=True)
        logger.warning("Server starting in degraded mode - crew initialization failed")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down Cardano API Server...")


# Main entry point
if __name__ == "__main__":
    import uvicorn

    logger.info("="*80)
    logger.info("Cardano CrewAI MCP API Server")
    logger.info("="*80)
    logger.info("Using CrewAI Agent with Cardano MCP tools")
    logger.info("Starting server on http://localhost:4326")
    logger.info("API Documentation: http://localhost:4326/docs")
    logger.info("="*80)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=4326,
        log_level="info",
        access_log=True
    )
