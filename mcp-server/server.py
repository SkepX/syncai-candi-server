import asyncio
import json
import logging
import math
import os
import time
import traceback
from datetime import datetime, timedelta
from functools import wraps
from typing import get_type_hints, get_origin, get_args, Dict, List, Optional, Tuple, Union, Any
from pydantic import Field
import inspect
import aiohttp
from dotenv import load_dotenv
import mcp.types as types
from supabase import create_client, Client
import redis.asyncio as redis
from tabulate import tabulate
from contextlib import asynccontextmanager
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import JSONResponse
from starlette.requests import Request


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("cardano-mcp")

# Load environment variables
load_dotenv()

print("✅ TAPTOOLS_API_KEY =", os.getenv("TAPTOOLS_API_KEY"))
print("✅ REDIS_PORT =", os.getenv("REDIS_PORT"))

#__________________constants_________________
#Mcp port
PORT= int(os.environ.get("PORT",4324))
# API keys and URLs
BLOCKFROST_API_KEY = os.getenv("BLOCKFROST_API_KEY")
CARDANOSCAN_API_KEY = os.getenv("CARDANOSCAN_API_KEY")
BLOCKFROST_URL = os.getenv("BLOCKFROST_URL", "https://cardano-mainnet.blockfrost.io/api/v0")
CARDANOSCAN_BASE_URL = os.getenv("CARDANOSCAN_BASE_URL", "https://api.cardanoscan.io")
# Add this to your environment variables section
TAPTOOLS_API_KEY = os.getenv("TAPTOOLS_API_KEY")
TAPTOOLS_BASE_URL = os.getenv("TAPTOOLS_BASE_URL", "https://openapi.taptools.io/api/v1")
REDIS_URL = os.getenv("REDIS_URL")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_PORT =int(os.getenv("REDIS_PORT","15387"))
# Global configuration
DEFAULT_CONFIG = {
    "page_size": 20,
    "max_text_length": 5000,
    "enable_chunking": True,
    "enable_pagination": True,
    "default_summary_mode": False
}


#syncai config
SYNCAI_BASE_URL = os.getenv("SYNCAI_BASE_URL",)
SYNCAI_KEY = os.getenv("SYNCAI_KEY")





# Function to apply configuration to all tools
def configure_output_handling(config=None):
    """Update the global output handling configuration"""
    global DEFAULT_CONFIG
    if config:
        DEFAULT_CONFIG.update(config)
    return DEFAULT_CONFIG

# Then use this in your tools
def get_config_value(param_name, provided_value=None):
    """Get a configuration value, with provided value taking precedence"""
    if provided_value is not None:
        return provided_value
    return DEFAULT_CONFIG.get(param_name)

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize MCP server
@asynccontextmanager
async def lifespan(app):
    await initialize_cache()
    yield  

from mcp.server.fastmcp import FastMCP
mcp = FastMCP("server", port = PORT, host = "0.0.0.0")

# Global cache state
supabase = None
cache_available = False
cache_init_lock = asyncio.Lock()  # Lock to prevent concurrent initializations
last_connection_attempt = 0
CONNECTION_RETRY_INTERVAL = 60  # seconds

# Tool Categories Metadata
# Organizes 86+ tools into logical categories for better discoverability
TOOL_CATEGORIES = {
    "governance": {
        "description": "DReps, SPOs, Constitutional Committee, voting, and governance proposals",
        "tools": [
            "search_dreps", "search_spos", "search_constitutional_committee",
            "search_proposals", "analyze_proposal", "analyze_voter_rationale",
            "search_governance_knowledge", "get_trend_data", "get_drep_country_distribution",
            "get_proposal_voting_history", "get_user_voting_history", "get_proposal_rationales",
            "get_user_rationales", "get_ada_power_history", "check_proposal_access"
        ],
        "use_when": "User asks about governance, DReps, SPOs, voting, proposals, committees"
    },
    "accounts": {
        "description": "Stake account analysis, rewards, delegation tracking, and history",
        "tools": [
            "account_overview", "account_rewards_analysis", "account_delegation_tracker",
            "account_history", "account_assets_summary"
        ],
        "use_when": "User asks about stake accounts, rewards, delegation, account history"
    },
    "addresses": {
        "description": "Address exploration, balances, UTXOs, transactions, and assets",
        "tools": [
            "address_explorer", "address_utxo_browser", "address_transaction_history",
            "address_assets_tracker"
        ],
        "use_when": "User asks about specific addresses, UTXOs, address balances, or transactions"
    },
    "tokens": {
        "description": "Token prices, holders, database lookup, technical analysis",
        "tools": [
            "token_database_lookup", "token_holders_analysis", "token_price_indicators",
            "loan_offers_analysis", "active_loans_analysis"
        ],
        "use_when": "User asks about tokens, prices, holders, token lookup, or DeFi loans"
    },
    "market": {
        "description": "Market statistics, DEX volumes, and on-chain activity",
        "tools": [
            "market_stats"
        ],
        "use_when": "User asks about market data, DEX volume, or overall network activity"
    },
    "nft": {
        "description": "NFT collections, volumes, floor prices, and trends",
        "tools": [
            # Will be populated as NFT tools are identified
        ],
        "use_when": "User asks about NFTs, collections, floor prices, or NFT market data"
    },
    "defi": {
        "description": "DeFi protocols, TVL, liquidity pools",
        "tools": [
            # Will be populated as DeFi tools are identified
        ],
        "use_when": "User asks about DeFi, TVL, liquidity, or protocol data"
    },
    "utility": {
        "description": "System health, configuration, and monitoring",
        "tools": [
            "health_check"
        ],
        "use_when": "User needs to check system status or troubleshoot"
    }
}

def get_tools_by_category(category: str) -> List[str]:
    """Get list of tools in a specific category"""
    return TOOL_CATEGORIES.get(category, {}).get("tools", [])

def get_category_for_tool(tool_name: str) -> Optional[str]:
    """Find which category a tool belongs to"""
    for category, data in TOOL_CATEGORIES.items():
        if tool_name in data.get("tools", []):
            return category
    return None


#Redis config
pool = redis.ConnectionPool(
    host=REDIS_URL,
    port=REDIS_PORT,
    decode_responses=True,
    username="default",
    password=REDIS_PASSWORD,
)


cache_db = None
# Utility functions
def rate_limit(calls_per_second: float):
    """
    Decorator that enforces a minimum delay between function calls.
    
    Args:
        calls_per_second: Maximum number of calls per second
    """
    min_interval = 1.0 / calls_per_second
    
    def decorator(func):
        last_call = [0.0]
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            
            elapsed = time.time() - last_call[0]
            wait_time = min_interval - elapsed
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            result = await func(*args, **kwargs)
            last_call[0] = time.time()
            
            return result
        
        return wrapper
    
    return decorator

# Helper function to safely convert lovelace to ADA
def lovelace_to_ada(lovelace):
    """Safely convert lovelace to ADA amount"""
    try:
        # Ensure lovelace is an integer
        lovelace_int = int(lovelace) if lovelace is not None else 0
        return lovelace_int / 1000000
    except (ValueError, TypeError):
        # In case of conversion error, return 0
        return 0


def create_task_decorator(func):
    """Decorator that wraps async function calls with asyncio.create_task()"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.create_task(func(*args, **kwargs))
    return wrapper

# Cache_results decorator redis based cache
def cache_results(table, service_name, ttl_hours: int = 24):
    def decorator(fetch_function):
        @wraps(fetch_function)
        async def wrapper(*args, **kwargs):
            try:
                global redis_cache

                sig = inspect.signature(fetch_function)
                params = list(sig.parameters)
                is_method = params and params[0] == "self"

                # Remove 'self' from args if it's a method
                args_list = list(args[1:] if is_method else args)
                args_list = [str(a) for a in args_list]
                kwargs_list = [str(v) for k, v in sorted(kwargs.items())]
                cache_key = ":".join(args_list+kwargs_list)
                list(kwargs.values())
                cached_data = None
                
                if not kwargs.get("ignore_cache",False):
                    cached_data = await redis_cache.get_cached_data(
                        table=table, key_field="cache_key", key_value=cache_key
                    )
                
                if "ignore_cache" in kwargs:
                    del kwargs["ignore_cache"]

                if cached_data:
                    return [cached_data, None,service_name]
                else:
                    results = await fetch_function(*args, **kwargs)
                    data= results[0]
                    err = results[1]
                    component = service_name
                    results = [data, err, component]
                    
                    if data:
                        
                        redis_cache.store_cached_data(
                            table=table,
                            key_field="cache_key",
                            key_value=cache_key,
                            data_items=data,
                            ttl_hours = ttl_hours
                        )
                    return results

            except Exception as e:
                logger.error(f"Error occured in {service_name}:-\n{e}")
                return [None, e, service_name]   
        return wrapper
    return decorator



# Response formating utils

def stringify_nested(data):
    result = []
    for row in data:
        clean_row = {}
        for k, v in row.items():
            if isinstance(v, (dict, list)):
                clean_row[k] = str(v)
            else:
                clean_row[k] = v
        result.append(clean_row)
    return result

def format_results(data):
    """
    Format the data in tabular form. Data needs to be dict of key,value pairs.
    For complex objects, it is recommended to do it manually.
    """
    try:
        if data is None:
            return "can't format None data"
        
        if isinstance(data, dict):
            data = [data]
        
        data = stringify_nested(data)
        
        if not data:
            return "No data returned"
        
        result = ""
        
        # Create separate table for each object
        for i, item in enumerate(data):
            if isinstance(item, dict):
                keys = list(item.keys())
                
                if not keys:
                    continue
                
                # Create header row
                header = "| " + " | ".join(keys) + " |"
                separator = "| " + " | ".join(["---"] * len(keys)) + " |"
                
                # Create data row
                row_values = []
                for key in keys:
                    value = item.get(key, "")
                    # Convert complex objects to string
                    if isinstance(value, (dict, list, tuple)):
                        value = str(value)
                    row_values.append(str(value))
                
                data_row = "| " + " | ".join(row_values) + " |"
                
                # Add table number if multiple tables
                if len(data) > 1:
                    result += f"**Table {i+1}:**\n"
                
                result += header + "\n" + separator + "\n" + data_row + "\n\n"
            else:
                result += f"{str(item)}\n\n"
        
        return result
        
    except Exception as e:
        logger.error(f"could not format data: {data} error: {e}")
        return str(data)

def format_results_decorator(func):
    """
        Format the data in tabular form. Data needs to be dict of key,value pairs.
        For compleax objects. It is recommended to do it manually.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            results = await func(*args, **kwargs)
            
            data= results[0]
            err = results[1]
            component = results[2]

            if err: return f"Could not get {component} err:{err}"
            return format_results(data)
        except Exception as e:
            
            logger.error(f"Could not format data {data}, error occured {e}")
            return data
    return wrapper

class PaginatedResponse:
    def __init__(self, data, page_size=20):
        self.data = data
        self.page_size = page_size
        self.total_items = len(data)
        self.total_pages = (self.total_items + page_size - 1) // page_size
        
    def get_page(self, page_num=1):
        """Get a specific page of data"""
        start_idx = (page_num - 1) * self.page_size
        end_idx = min(start_idx + self.page_size, self.total_items)
        return {
            "items": self.data[start_idx:end_idx],
            "page": page_num,
            "total_pages": self.total_pages,
            "total_items": self.total_items,
            "items_per_page": self.page_size
        }
        
    def get_summary(self):
        """Get a summary of the data without the full contents"""
        return {
            "total_items": self.total_items,
            "total_pages": self.total_pages,
            "items_per_page": self.page_size
        }


# API client classes
class BlockfrostAPI:
    """Client for the Blockfrost API."""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "project_id": api_key,
            "Content-Type": "application/json"
        }
    
    # In your BlockfrostAPI class
    async def get(self, endpoint: str, params: Optional[Dict] = None, timeout: float = 30.0, retries: int = 2) -> Any:
        """
        Makes a GET request to the Blockfrost API with timeout handling and retries.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters
            timeout: Request timeout in seconds
            retries: Number of retry attempts for timeout errors
                
        Returns:
            API response as JSON or error object
        """
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"Calling Blockfrost API: {endpoint}")
        
        retry_count = 0
        while retry_count <= retries:
            try:
                timeout_ctx = aiohttp.ClientTimeout(total=timeout)
                async with aiohttp.ClientSession(timeout=timeout_ctx) as session:
                    async with session.get(url, headers=self.headers, params=params) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 408 or response.status == 504:
                            # Handle timeout status codes
                            retry_count += 1
                            if retry_count <= retries:
                                logger.warning(f"Request timeout for {endpoint}, retrying ({retry_count}/{retries})...")
                                await asyncio.sleep(1.0 * retry_count)  # Exponential backoff
                                continue
                            else:
                                return {"error": "API timeout", "details": f"Request timed out after {retries} retries"}
                        else:
                            error_text = await response.text()
                            logger.error(f"Error from Blockfrost API: {response.status} - {error_text}")
                            return {"error": f"API error: {response.status}", "details": error_text}
            except asyncio.TimeoutError:
                # Handle asyncio timeout
                retry_count += 1
                if retry_count <= retries:
                    logger.warning(f"Request timeout for {endpoint}, retrying ({retry_count}/{retries})...")
                    await asyncio.sleep(1.0 * retry_count)
                    continue
                else:
                    return {"error": "API timeout", "details": f"Request timed out after {retries} retries"}
            except Exception as e:
                logger.error(f"Exception calling Blockfrost API: {e}")
                return {"error": f"Exception: {str(e)}"}


class CardanoScanAPI:
    """Client for the CardanoScan API."""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
    
    @rate_limit(5)  # 5 calls per second maximum
    async def get_governance_action(self, action_id: str) -> Tuple[Any, Optional[str]]:
        """
        Retrieves Governance Action details with rate limiting.
        
        Args:
            action_id: The action ID (txhash + "00")
            
        Returns:
            Tuple of (data, error)
        """
        url = f"{self.base_url}/governance/action"
        headers = {"apiKey": self.api_key}
        params = {"actionId": action_id}
        logger.info(f"Calling CardanoScan Governance Action endpoint: {action_id}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        text = await response.text()
                        if not text.strip():
                            logger.warning("Empty response text for Governance Action.")
                            return None, "Empty response text."
                        try:
                            return await response.json(), None
                        except json.decoder.JSONDecodeError as e:
                            err = f"Error decoding JSON: {e}\nRaw response: {text}"
                            logger.error(err)
                            return None, err
                    else:
                        err = f"Error fetching Governance Action. Status code: {response.status}\nRaw response: {await response.text()}"
                        logger.error(err)
                        return None, err
        except Exception as e:
            err = f"Exception fetching Governance Action: {e}"
            logger.error(err)
            return None, err

    @rate_limit(5)  # 5 calls per second maximum
    async def get_governance_committee(self) -> Tuple[Any, Optional[str]]:
        """
        Retrieves Committee Information with rate limiting.
        
        Returns:
            Tuple of (data, error)
        """
        url = f"{self.base_url}/governance/committee"
        headers = {"apiKey": self.api_key}
        logger.info("Calling CardanoScan Committee Information endpoint.")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.json(), None
                    else:
                        err = f"Error fetching Committee Information. Status code: {response.status}"
                        logger.error(err)
                        return None, err
        except Exception as e:
            err = f"Exception fetching Committee Information: {e}"
            logger.error(err)
            return None, err

async def make_get_request(service_name:str, url:str, params:dict,headers:str):
    logger.info(f"Calling {service_name}")
    logger.info(f"Making Get request to: {url} with params: {json.dumps(params)}")
    logger.info(f"With headers: {headers}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params) as response:
                response_text = await response.text()
                logger.info(f"Response status: {response.status}")
                logger.info(f"Response headers: {response.headers}")
                logger.info(f"Response body: {response_text[:200]}...")  # Log first 200 chars
                    
                if response.status == 200:
                    return await response.json(), None,service_name
                else:
                    err = f"Error fetching {service_name}. Status code: {response.status}\nRaw response: {response_text}"
                    logger.error(err)
                    return None, err,service_name
    except Exception as e:
        err = f"Exception fetching {service_name}: {e}"
        logger.error(err)
        return None, err,service_name


async def make_post_request(service_name: str, url: str, body: dict, headers: dict = None):
    logger.info(f"Calling {service_name}")
    logger.info(f"Making POST request to: {url}")
    logger.info(f"Request body: {json.dumps(body)}")
    logger.info(f"With headers: {headers}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=body) as response:
                response_text = await response.text()
                logger.info(f"Response status: {response.status}")
                logger.info(f"Response headers: {response.headers}")
                logger.info(f"Response body: {response_text[:200]}...")  # Log first 200 chars
                
                if response.status == 200:
                    return await response.json(), None, service_name
                else:
                    err = f"Error fetching {service_name}. Status code: {response.status}\nRaw response: {response_text}"
                    logger.error(err)
                    return None, err, service_name
    except Exception as e:
        err = f"Exception fetching {service_name}: {e}"
        logger.error(err)
        return None, err, service_name
class TapToolsAPI:
    """Client for the TapTools API."""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "x-api-key": api_key,
            "Content-Type": "application/json"
        }
    
    @rate_limit(5)  # 5 calls per second maximum
    async def get_token_holders(self, unit: str,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), per_page: int = 20) -> Tuple[Any, Optional[str]]:
        """
        Retrieves top token holders for a specific token.
        
        Args:
            unit: The token unit (policy ID + hex name)
            page: Page number for pagination
            per_page: Number of items per page (max 100)
            
        Returns:
            Tuple of (data, error)
        """
        url = f"{self.base_url}/token/holders/top"
        params = {
            "unit": unit,
            "page": page,
            "perPage": min(per_page, 100)  # Enforce maximum of 100 per page
        }
        
        # Log the full request details for debugging
        full_url = f"{url}?unit={unit}&page={page}&perPage={min(per_page, 100)}"
        logger.info(f"Making request to: {full_url}")
        logger.info(f"With headers: {self.headers}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as response:
                    response_text = await response.text()
                    logger.info(f"Response status: {response.status}")
                    logger.info(f"Response headers: {response.headers}")
                    logger.info(f"Response body: {response_text[:200]}...")  # Log first 200 chars
                    
                    if response.status == 200:
                        return await response.json(), None
                    else:
                        err = f"Error fetching token holders. Status code: {response.status}\nRaw response: {response_text}"
                        logger.error(err)
                        return None, err
        except Exception as e:
            err = f"Exception fetching token holders: {e}"
            logger.error(err)
            return None, err
            

    @rate_limit(5)  # 5 calls per second maximum
    async def get_loan_offers(self, unit: str, include: str = "collateral,debt", sort_by: str = "time", 
                            order: str = "desc",  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), per_page: int = 100) -> Tuple[Any, Optional[str]]:
        """
        Retrieves P2P loan offers for a specific token.
        
        Args:
            unit: The token unit (policy ID + hex name)
            include: Filter to offers where token is used as 'collateral', 'debt', 'interest' or a mix
            sort_by: What to sort results by ('time' or 'duration')
            order: Sort direction ('asc' or 'desc')
            page: Page number for pagination
            per_page: Number of items per page (max 100)
            
        Returns:
            Tuple of (data, error)
        """
        url = f"{self.base_url}/token/debt/offers"
        params = {
            "unit": unit,
            "include": include,
            "sortBy": sort_by,
            "order": order,
            "page": page,
            "perPage": min(per_page, 100)  # Enforce maximum of 100 per page
        }
        
        logger.info(f"Calling TapTools loan offers endpoint for token: {unit}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as response:
                    response_text = await response.text()
                    logger.info(f"Response status: {response.status}")
                    
                    if response.status == 200:
                        return await response.json(), None
                    else:
                        err = f"Error fetching loan offers. Status code: {response.status}\nRaw response: {response_text}"
                        logger.error(err)
                        return None, err
        except Exception as e:
            err = f"Exception fetching loan offers: {e}"
            logger.error(err)
            return None, err

    # Add this method to the TapToolsAPI class
    @rate_limit(5)  # 5 calls per second maximum
    async def get_loan_offers(self, unit: str, include: str = "collateral,debt", sort_by: str = "time", 
                            order: str = "desc",  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), per_page: int = 100) -> Tuple[Any, Optional[str]]:
        """
        Retrieves P2P loan offers for a specific token.
        
        Args:
            unit: The token unit (policy ID + hex name)
            include: Filter to offers where token is used as 'collateral', 'debt', 'interest' or a mix
            sort_by: What to sort results by ('time' or 'duration')
            order: Sort direction ('asc' or 'desc')
            page: Page number for pagination
            per_page: Number of items per page (max 100)
            
        Returns:
            Tuple of (data, error)
        """
        url = f"{self.base_url}/token/debt/offers"
        params = {
            "unit": unit,
            "include": include,
            "sortBy": sort_by,
            "order": order,
            "page": page,
            "perPage": min(per_page, 100)  # Enforce maximum of 100 per page
        }
        
        # Log the full request details for debugging
        full_url = f"{url}?unit={unit}&include={include}&sortBy={sort_by}&order={order}&page={page}&perPage={min(per_page, 100)}"
        logger.info(f"Making request to: {full_url}")
        logger.info(f"With headers: {self.headers}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as response:
                    response_text = await response.text()
                    logger.info(f"Response status: {response.status}")
                    logger.info(f"Response headers: {response.headers}")
                    logger.info(f"Response body: {response_text[:200]}...")  # Log first 200 chars
                    
                    if response.status == 200:
                        return await response.json(), None
                    else:
                        err = f"Error fetching loan offers. Status code: {response.status}\nRaw response: {response_text}"
                        logger.error(err)
                        return None, err
        except Exception as e:
            err = f"Exception fetching loan offers: {e}"
            logger.error(err)
            return None, err

    # Add this method to the TapToolsAPI class
    @rate_limit(5)  # 5 calls per second maximum
    async def get_active_loans(
        self, 
        unit: str, 
        include: str = "collateral,debt",
        sort_by: str = "time",
        order: str = "desc",
         page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
        per_page: int = 100
    ) -> Tuple[Any, Optional[str]]:
        """
        Retrieves active P2P loans of a specific token.
        
        Args:
            unit: The token unit (policy ID + hex name)
            include: Filter to loans where token is used as 'collateral', 'debt', 'interest' or a mix
            sort_by: What to sort results by ('time' or 'expiration')
            order: Sort direction ('asc' or 'desc')
            page: Page number for pagination
            per_page: Number of items per page (max 100)
                
        Returns:
            Tuple of (data, error)
        """
        url = f"{self.base_url}/token/debt/loans"
        params = {
            "unit": unit,
            "include": include,
            "sortBy": sort_by,
            "order": order,
            "page": page,
            "perPage": min(per_page, 100)  # Enforce maximum of 100 per page
        }
        
        # Log the full request details for debugging
        full_url = f"{url}?unit={unit}&include={include}&sortBy={sort_by}&order={order}&page={page}&perPage={min(per_page, 100)}"
        logger.info(f"Making request to: {full_url}")
        logger.info(f"With headers: {self.headers}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as response:
                    response_text = await response.text()
                    logger.info(f"Response status: {response.status}")
                    logger.info(f"Response headers: {response.headers}")
                    logger.info(f"Response body: {response_text[:200]}...")  # Log first 200 chars
                    
                    if response.status == 200:
                        return await response.json(), None
                    else:
                        err = f"Error fetching active loans. Status code: {response.status}\nRaw response: {response_text}"
                        logger.error(err)
                        return None, err
        except Exception as e:
            err = f"Exception fetching active loans: {e}"
            logger.error(err)
            return None, err


    # Add this method to the TapToolsAPI class
    # Add this method to the TapToolsAPI class
    @rate_limit(5)  # 5 calls per second maximum
    async def get_token_indicators(
        self, 
        unit: str, 
        interval: str, 
        indicator: str = None,
        items: int = None,
        length: int = None,
        smoothing_factor: int = None,
        fast_length: int = None,
        slow_length: int = None,
        signal_length: int = None,
        std_mult: int = None,
        quote: str = None
    ) -> Tuple[Any, Optional[str]]:
        """
        Retrieves token price indicators (e.g., MA, EMA, RSI, MACD, BB, BBW).
        
        Args:
            unit: The token unit (policy ID + hex name)
            interval: Time interval (3m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 3d, 1w, 1M)
            indicator: Which indicator to use (ma, ema, rsi, macd, bb, bbw)
            items: Number of items to return (max 1000)
            length: Length of data (used in ma, ema, rsi, bb, bbw)
            smoothing_factor: Smoothing factor for EMA (typically 2)
            fast_length: Length of shorter EMA for MACD
            slow_length: Length of longer EMA for MACD
            signal_length: Length of signal EMA for MACD
            std_mult: Standard deviation multiplier for Bollinger Bands
            quote: Quote currency (e.g., ADA, USD)
            
        Returns:
            Tuple of (data, error)
        """
        url = f"{self.base_url}/token/indicators"
        params = {
            "unit": unit,
            "interval": interval
        }
        
        # Add optional parameters if provided
        if indicator:
            params["indicator"] = indicator
        if items:
            params["items"] = min(items, 1000)  # Enforce maximum of 1000
        if length:
            params["length"] = length
        if smoothing_factor:
            params["smoothingFactor"] = smoothing_factor
        if fast_length:
            params["fastLength"] = fast_length
        if slow_length:
            params["slowLength"] = slow_length
        if signal_length:
            params["signalLength"] = signal_length
        if std_mult:
            params["stdMult"] = std_mult
        if quote:
            params["quote"] = quote
        
        # Log the full request details for debugging
        full_url = f"{url}?unit={unit}&interval={interval}"
        if indicator:
            full_url += f"&indicator={indicator}"
        # Add other parameters to the log as needed
        
        logger.info(f"Making request to: {full_url}")
        logger.info(f"With headers: {self.headers}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, params=params) as response:
                    response_text = await response.text()
                    logger.info(f"Response status: {response.status}")
                    logger.info(f"Response headers: {response.headers}")
                    logger.info(f"Response body: {response_text[:200]}...")  # Log first 200 chars
                    
                    if response.status == 200:
                        return await response.json(), None
                    else:
                        err = f"Error fetching token indicators. Status code: {response.status}\nRaw response: {response_text}"
                        logger.error(err)
                        return None, err
        except Exception as e:
            err = f"Exception fetching token indicators: {e}"
            logger.error(err)
            return None, err
    
    #__________________________Market Stats______________________________

    @cache_results(table = "market_stats", service_name = "Market Stats")
    @rate_limit(5)
    async def get_market_stats(self,quote:str):
        """
        Get aggregated market stats, including 24h DEX volume and total active addresses onchain. Active addresses are addresses that have either sent or received any transactions within the last 24 hours. Multiple addresses with the same stake key will be counted as one address.
        
        Args:
            quote: str (# Example: quote=ADA Quote currency to use (ADA, USD, EUR, ETH, BTC).        Default  is ADA.)

        """
        url= f"{self.base_url}/market/stats"
        params= {
            "quote":quote
        }
        stats = await make_get_request("Market Stats", url, params,self.headers)
        return stats
    

    #______________nft_____________________________________________
    @cache_results(table="nft_sale_history", service_name = "nft Sale History")
    @rate_limit(5)
    async def get_nft_sale_history(self,**params):
        """
        Get a specific asset's sale history.

        Args:-
        policy : string (required) (Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728
        The policy ID for the collection)

        name :string (Example: name=ClayNation3725 The name of a specific nft to get stats for)
        """
        
        url= f"{self.base_url}/nft/asset/sales"
        
        

        response = await make_get_request("nft Sale History", url, params,self.headers)
        return response
    
    @cache_results(table="nft_stats", service_name="nft Stats")
    @rate_limit(5)
    async def get_nft_stats(self,**params):
        """
        Get high-level stats on a certain nft asset.

        Args:
            policy: str (# Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.)
            name: str (# Example: name=ClayNation3725 The name of a specific nft to get stats for.)
        """
        url = f"{self.base_url}/nft/asset/stats"
        
        stats = await make_get_request("nft Stats", url, params, self.headers)
        return stats


    @cache_results(table="nft_traits", service_name="nft Traits")
    @rate_limit(5)
    async def get_nft_traits(self,**params):
        """
        Get a specific nft's traits and trait prices.

        Args:
            policy: str (# Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.)
            name: str (# Example: name=ClayNation3725 The name of a specific nft to get stats for.)
            prices: str (# Example: prices=0 Include trait prices or not, Options are 0, 1. Default is 1.)
        """
        url = f"{self.base_url}/nft/asset/traits"
        
        traits = await make_get_request("nft Traits", url, params, self.headers)
        return traits


    @cache_results(table="collection_assets", service_name="Collection Assets")
    @rate_limit(5)
    async def get_collection_assets(self,**params):
        """
        Get all nfts from a collection with the ability to sort by price/rank and filter to specific traits.

        Args:
            search: str (# Example: c Search for a certain nft's name.)
            policy: str (# Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.)
            sortBy: str (# Example: sortBy=price What should the results be sorted by. Options: price, rank.)
            order: str (# Example: order=asc Which direction should the results be sorted. Options: asc, desc.)
            
            onSale: str (# Example: onSale=1 Return only nfts that are on sale. Options: 0, 1.)
            page: int (# Example: page=1 Pagination support.)
            perPage: int (# Example: perPage=100 Number of items per page. Max is 100.)
            traits: dict (# Trait filters as additional keyword arguments.)
        """
        url = f"{self.base_url}/nft/collection/assets"
        
        assets = await make_get_request("Collection Assets", url, params, self.headers)
        return assets


    @cache_results(table="nft_holder_distribution", service_name="Holder Distribution")
    @rate_limit(5)
    async def get_nft_holder_distribution(self,**params):
        """
        Get the distribution of nfts within a collection by bucketing into number of nfts held groups.

        Args:
            policy: str (# Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.)
        """
        url = f"{self.base_url}/nft/collection/holders/distribution"
        
        distribution = await make_get_request("Holder Distribution", url, params, self.headers)
        return distribution

    @cache_results(table="nft_top_holders", service_name="nft Top Holders")
    @rate_limit(5)
    async def get_nft_top_holders(self, **params):
        """
        Get the top holders for a particular nft collection.
        This includes owners with listed or staked nfts.

        Args:
            policy: str
                Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728
                The policy ID for the collection.

            page: int, optional
                Example: page=1
                This endpoint supports pagination. Default is 1.

            perPage: int, optional
                Example: perPage=10
                Specify how many items to return per page. Max is 100, default is 10.

            excludeExchanges: int, optional
                Example: excludeExchanges=1
                Whether or not to exclude marketplace addresses (0, 1)

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/holders/top"

        top_holders = await make_get_request("nft Top Holders", url, params, self.headers)
        return top_holders


    @cache_results(table="nft_holder_trend", service_name="nft Holder Trend")
    @rate_limit(5)
    async def get_nft_holder_trend(self, **params):
        """
        Get holders trended by day for a particular nft collection.
        This includes owners with listed or staked nfts.

        Args:
            policy: str
                Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728
                The policy ID for the collection.

            timeframe: str, optional
                Example: timeframe=30d
                The time interval. Options: 7d, 30d, 90d, 180d, 1y, all. Defaults to 30d.

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/holders/trended"

        holder_trend = await make_get_request("nft Holder Trend", url, params, self.headers)
        return holder_trend


    @cache_results(table="nft_collection_info", service_name="nft Collection Info")
    @rate_limit(5)
    async def get_nft_collection_info(self, **params):
        """
        Get basic information about a collection like name, socials, and logo.

        Args:
            policy: str
                Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                The policy ID for the collection.

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/info"

        collection_info = await make_get_request("nft Collection Info", url, params, self.headers)
        return collection_info

    @cache_results(table="number_of_active_listing", service_name="Number of Active Listing")
    @rate_limit(5)
    async def get_number_of_active_listing(self, **params):
        """
        Get the number of active listings and total supply for a particular nft collection.

        Args:
            policy: str
                Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                The policy ID for the collection.

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            dict:
                A dictionary containing:
                - 'listings': Number of active listings in the collection.
                - 'supply': Total number of nfts minted/supplied in the collection.
        """
        url = f"{self.base_url}/nft/collection/listings"
        
        active_listing_info = await make_get_request("Number of Active Listing", url, params, self.headers)
        return active_listing_info

    @cache_results(table="nft_listings_depth", service_name="nft Listings Depth")
    @rate_limit(5)
    async def get_nft_listings_depth(self, **params):
        """
            Get cumulative amount of listings at each price point, starting at the floor and moving upwards.

            Args:
                policy: str
                    Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                    The policy ID for the collection.

                items: int, optional
                    Example: items=600
                    Specify how many items to return. Maximum is 1000, default is 500.
    
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/listings/depth"
    
        listings_depth = await make_get_request("nft Listings Depth", url, params, self.headers)
        return listings_depth


    @cache_results(table="nft_active_listings", service_name="nft Active Listings")
    @rate_limit(5)
    async def get_nft_active_listings(self, **params):
        """
            Get a list of active nft listings with supporting information. Supports pagination and sorting.

            Args:
                policy: str
                    Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                    The policy ID for the collection.

                sortBy: str, optional
                    Example: sortBy=price
                    Sort results by 'price' or 'time'. Default is 'price'.

                order: str, optional
                    Example: order=asc
                    Sorting direction, either 'asc' or 'desc'. Default is 'asc'.

                page: int, optional
                    Example: page=1
                    Page number for pagination. Default is 1.

                perPage: int, optional
                    Example: perPage=100
                    Number of items per page. Max is 100, default is 100.

                ignore_cache: bool, optional
                    Default: False
                    Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/listings/individual"
        active_listings = await make_get_request("nft Active Listings", url, params, self.headers)
        return active_listings


    @cache_results(table="nft_listings_trended", service_name="nft Listings Trended")
    @rate_limit(5)
    async def get_nft_listings_trended(self, **params):
        """
            Get trended number of listings and floor price for a particular nft collection.

            Args:
                policy: str
                    Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                    The policy ID for the collection.

                interval: str
                    Example: interval=1d
                    Time interval for the trend. Options: 3m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 3d, 1w, 1M.

                numIntervals: int, optional
                    Example: numIntervals=180
                    Number of intervals to return. Leave blank to get full history.

                ignore_cache: bool, optional
                    Default: False
                    Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/listings/trended"
        trended_listings = await make_get_request("nft Listings Trended", url, params, self.headers)
        return trended_listings


    @cache_results(table="nft_floor_price_ohlcv", service_name="nft Floor Price OHLCV")
    @rate_limit(5)
    async def get_nft_floor_price_ohlcv(self, **params):
        """
        Get OHLCV (open, high, low, close, volume) data of floor price for a particular nft collection.

        Can be used to create candlestick or line charts based on price trends.

        Args:
            policy: str
                Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                The policy ID for the collection.

            interval: str
                Example: interval=1d
                Time interval for OHLCV data. Options: 3m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 3d, 1w, 1M.

            numIntervals: int, optional
                Example: numIntervals=180
                Number of intervals to return.

            quote: str, optional
                Example: quote=ada
                Quote currency to use (e.g., ADA, USD, EUR). Defaults to ADA.

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/ohlcv"
        ohlcv_data = await make_get_request("nft Floor Price OHLCV", url, params, self.headers)
        return ohlcv_data


    @cache_results(table="nft_collection_stats", service_name="nft Collection Stats")
    @rate_limit(5)
    async def get_nft_collection_stats(self, **params):
        """
        Get basic statistics about an nft collection including floor price, volume, supply, and more.

        Args:
            policy: str
                Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                The policy ID for the collection.

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/stats"
        collection_stats = await make_get_request("nft Collection Stats", url, params, self.headers)
        return collection_stats

    @cache_results(table="nft_collection_stats_extended", service_name="nft Collection Stats Extended")
    @rate_limit(5)
    async def get_nft_collection_stats_extended(self, **params):
        """
        Get basic information about a collection like floor price, volume, and supply.
        Volume is lifetime. pctChg values are % differences between current and timeframe value.

        Args:
            policy: str
                Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                The policy ID for the collection.

            timeframe: str, optional
                Example: timeframe=24h
                The time interval. Options: 24h, 7d, 30d. Default is 24h.

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/stats/extended"
        return await make_get_request("nft Collection Stats Extended", url, params, self.headers)


    @cache_results(table="nft_collection_trades", service_name="nft Collection Trades")
    @rate_limit(5)
    async def get_nft_collection_trades(self, **params):
        """
        Get individual trades for a collection or entire market if policy not provided.

        Args:
            policy: str, optional
            timeframe: str, optional
                Example: timeframe=30d
                Options: 1h, 4h, 24h, 7d, 30d, 90d, 180d, 1y, all. Default: 30d.
            sortBy: str, optional
                Example: sortBy=time
            order: str, optional
                Example: order=desc
            minAmount: int, optional
            from: int, optional
                UNIX timestamp for filtering.
            page: int, optional
            perPage: int, optional
            ignore_cache: bool, optional
        """
        url = f"{self.base_url}/nft/collection/trades"
        if "from_timestamp" in params:
            params["from"] = params[f"from_timestamp"]
            del params[f"from_timestamp"]
        return await make_get_request("nft Collection Trades", url, params, self.headers)


    @cache_results(table="nft_collection_trades_stats", service_name="nft Collection Trades Stats")
    @rate_limit(5)
    async def get_nft_collection_trades_stats(self, **params):
        """
        Get trading stats like volume and number of sales for a collection.

        Args:
            policy: str
                Required. Policy ID.
            timeframe: str, optional
                Example: timeframe=24h
                Options: 1h, 4h, 24h, 7d, 30d, all. Default: 24h.
            ignore_cache: bool, optional
        """
        url = f"{self.base_url}/nft/collection/trades/stats"
        return await make_get_request("nft Collection Trades Stats", url, params, self.headers)


    @cache_results(table="nft_collection_traits_price", service_name="nft Collection Traits Price")
    @rate_limit(5)
    async def get_nft_collection_traits_price(self, **params):
        """
        Get a list of traits within a collection and each trait's floor price.

        Args:
            policy: str
                Required. Policy ID for the collection.
            name: str, optional
                nft name to get trait prices for.
            ignore_cache: bool, optional
        """
        url = f"{self.base_url}/nft/collection/traits/price"
        return await make_get_request("nft Collection Traits Price", url, params, self.headers)


    @cache_results(table="nft_collection_traits_rarity", service_name="nft Collection Traits Rarity")
    @rate_limit(5)
    async def get_nft_collection_metadata_rarity(self, **params):
        """
        Get every metadata attribute and how likely it is to occur in the collection.

        Args:
            policy: str
                Required. Policy ID for the collection.
            ignore_cache: bool, optional
        """
        url = f"{self.base_url}/nft/collection/traits/rarity"
        return await make_get_request("nft Collection Traits Rarity", url, params, self.headers)

    @cache_results(table="nft_rarity_rank", service_name="nft Rarity Rank")
    @rate_limit(5)
    async def get_nft_rarity_rank(self, **params):
        """
        Get rank of nft's rarity within a collection.
        Args:
            policy: str
                Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728
                The policy ID for the collection.
            name: str
                Example: name=ClayNation3725
                The name of the nft.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/traits/rarity/rank"
        rarity_rank = await make_get_request("nft Rarity Rank", url, params, self.headers)
        return rarity_rank


    @cache_results(table="nft_volume_trended", service_name="nft Volume Trended")
    @rate_limit(5)
    async def get_nft_volume_trended(self, **params):
        """
        Get trended volume and number of sales for a particular nft collection.
        Args:
            policy: str
                Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                The policy ID for the collection.
            interval: str
                Example: interval=1d
                The time interval. Options are `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `12h`, `1d`, `3d`, `1w`, `1M`.
            numIntervals: int, optional
                Example: numIntervals=180
                The number of intervals to return, e.g. if you want 180 days of data in 1d intervals, then pass `180` here.
                Leave blank for full history.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/collection/volume/trended"
        if params["numIntervals"] is None:
            del params["numIntervals"]
        volume_trended = await make_get_request("nft Volume Trended", url, params, self.headers)
        return volume_trended


    @cache_results(table="nft_market_stats", service_name="nft Market Stats")
    @rate_limit(5)
    async def get_nft_market_stats(self, **params):
        """
        Get high-level market stats across the entire nft market.

        'addresses' is the count of stake keys that have purchased or sold an nft 
        within the given timeframe. If an address does not have a stake key, 
        it is still counted as a distinct address.

        Args:
            timeframe: str
                Example: timeframe=1d
                The time interval. Options are `1h`, `4h`, `24h`, `7d`, `30d`, `all`. 
                Defaults to `24h`.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/market/stats"
        market_stats = await make_get_request("nft Market Stats", url, params, self.headers)
        return market_stats


    @cache_results(table="nft_market_stats_extended", service_name="nft Market Stats Extended")
    @rate_limit(5)
    async def get_nft_market_stats_extended(self, **params):
        """
        Get high-level market stats across the entire nft market with percentage changes.

        'addresses' is the count of stake keys that have purchased or sold an nft 
        within the given timeframe. If an address does not have a stake key, 
        it is still counted as a distinct address.

        Args:
            timeframe: str
                Example: timeframe=1d
                The time interval. Options are `1h`, `4h`, `24h`, `7d`, `30d`, `all`. 
                Defaults to `24h`.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/market/stats/extended"
        market_stats_extended = await make_get_request("nft Market Stats Extended", url, params, self.headers)
        return market_stats_extended


    @cache_results(table="nft_market_volume_trended", service_name="nft Market Volume Trended")
    @rate_limit(5)
    async def get_nft_market_volume_trended(self, **params):
        """
        Get trended volume for entire nft market.

        Args:
            timeframe: str
                Example: timeframe=30d
                The time interval. Options are `7d`, `30d`, `90d`, `180d`, `1y`, `all`. 
                Defaults to `30d`.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/market/volume/trended"
        market_volume_trended = await make_get_request("nft Market Volume Trended", url, params, self.headers)
        return market_volume_trended


    @cache_results(table="nft_marketplace_stats", service_name="nft Marketplace Stats")
    @rate_limit(5)
    async def get_nft_marketplace_stats(self, **params):
        """
        Get high-level nft marketplace stats.

        Args:
            timeframe: str, optional
                Example: timeframe=30d
                The time interval. Options are `24h`, `7d`, `30d`, `90d`, `180d`, `all`. 
                Defaults to `7d`.
            marketplace: str, optional
                Example: marketplace=jpg.store
                Filters data to a certain marketplace by name.
            lastDay: int, optional
                Example: lastDay=0
                Filters to only count data that occurred between yesterday 00:00UTC 
                and today 00:00UTC `(0,1)`.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
        url = f"{self.base_url}/nft/marketplace/stats"
        marketplace_stats = await make_get_request("nft Marketplace Stats", url, params, self.headers)
        return marketplace_stats
    

    @cache_results(table="nft_top_rankings", service_name="NFT Top Rankings")
    @rate_limit(5)
    async def get_nft_top_rankings(self, **params):
        """
        Get top NFT rankings based on total market cap, 24-hour volume, or 24-hour top price gainers/losers.

        Args:
            ranking: str, required
                Example: ranking=marketCap
                Criteria to rank NFT collections based on. Options are marketCap, volume, gainers, losers.
            items: int, optional
                Example: items=50
                Specify how many items to return. Maximum is 100, default is 25.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.

        Returns:
            NFT top rankings data.
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/nft/top/timeframe"
        response = await make_get_request("NFT Top Rankings", url, params, self.headers)
        return response

    #_________________________Token Methods______________________________

    @cache_results(table="nft_top_volume_collections", service_name="Top Volume Collections")
    @rate_limit(5)
    async def get_nft_top_volume_collections(self, **params):
        """
        Get top NFT collections by trading volume over a specified timeframe.

        Args:
            timeframe: str, optional
                Example: timeframe=24h
                The timeframe for volume aggregation. Options are:
                - 1h
                - 4h
                - 24h
                - 7d
                - 30d
                - all
                Defaults to 24h.
            page: int, optional
                Example: page=1
                Page number for pagination. Default is 1.
            perPage: int, optional
                Example: perPage=10
                Number of items to return per page (max: 100, default: 10).
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.

        Returns:
            Top NFT collections data.
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/nft/top/volume"
        response = await make_get_request("Top Volume Collections", url, params, self.headers)
        return response


    @cache_results(table="nft_top_volume_collections_extended", service_name="Top Volume Collections (Extended)")
    @rate_limit(5)
    async def get_nft_top_volume_collections_extended(self, **params):
        """
        Get top NFT collections by trading volume over a specified timeframe, including percentage change metrics.

        Args:
            timeframe: str, optional
                Example: timeframe=24h
                The timeframe for volume aggregation. Options are:
                - 1h
                - 4h
                - 24h
                - 7d
                - 30d
                - all
                Defaults to 24h.
            page: int, optional
                Example: page=1
                Page number for pagination. Default is 1.
            perPage: int, optional
                Example: perPage=10
                Number of items to return per page (max: 100, default: 10).
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.

        Returns:
            Top NFT collections data with percentage changes.
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/nft/top/volume/extended"
        response = await make_get_request("Top Volume Collections (Extended)", url, params, self.headers)
        return response


    @cache_results(table="token_links", service_name="Token Links")
    @rate_limit(5)
    async def get_token_links(self, **params):
        """
        Get a specific token's social links, if they have been provided to TapTools.

        Args:
            unit: str, required
                Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
                Token unit (policy + hex name).
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.

        Returns:
            A dictionary containing social links and description for the specified token.
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/links"
        response = await make_get_request("Token Links", url, params, self.headers)
        return response


    @cache_results(table="token_market_cap", service_name="Token Market Cap")
    @rate_limit(5)
    async def get_token_market_cap(self, **params):
        """
        Get a specific token's supply and market cap information.

        Args:
            unit: str, required
                Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
                Token unit (policy + hex name).
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.

        Returns:
            A dictionary containing the token's circulating supply, market cap, and price.
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/mcap"
        response = await make_get_request("Token Market Cap", url, params, self.headers)
        return response


    @cache_results(table="token_price_ohlcv", service_name="Token Price OHLCV")
    @rate_limit(5)
    async def get_token_price_ohlcv(self, **params):
        """
        Get a specific token's trended (open, high, low, close, volume) price data. You can either pass a token unit 
        to get aggregated data across all liquidity pools, or an onchainID for a specific pair.
        
        Args:
            unit: str, optional
                Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
                Token unit (policy + hex name).
            onchainID: str, optional
                Example: onchainID=0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1.39b9b709ac8605fc82116a2efc308181ba297c11950f0f350001e28f0e50868b
                Pair onchain ID to get ohlc data for.
            interval: str, required
                Example: interval=1d
                The time interval. Options are `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `12h`, `1d`, `3d`, `1w`, `1M`.
            numIntervals: int, optional
                Example: numIntervals=180
                The number of intervals to return, e.g. if you want 180 days of data in 1d intervals, then pass `180` here.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of dictionaries containing OHLCV data with the following keys:
            - close: float, closing price
            - high: float, highest price in the interval
            - low: float, lowest price in the interval
            - open: float, opening price
            - time: int, timestamp of the interval
            - volume: float, trading volume in the interval
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/ohlcv"
        response = await make_get_request("Token Price OHLCV", url, params, self.headers)
        return response


    @cache_results(table="token_liquidity_pools", service_name="Token Liquidity Pools")
    @rate_limit(5)
    async def get_token_liquidity_pools(self, **params):
        """
        Get a specific token's active liquidity pools. Can search for all token pools using unit or 
        can search for specific pool with onchainID.
        
        Args:
            unit: str, optional
                Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
                Token unit (policy + hex name).
            onchainID: str, optional
                Example: onchainID=0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1.39b9b709ac8605fc82116a2efc308181ba297c11950f0f350001e28f0e50868b
                Liquidity pool onchainID.
            adaOnly: int, optional
                Example: adaOnly=1
                Return only ADA pools or all pools (0, 1).
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of dictionaries containing liquidity pool information with the following keys:
            - exchange: str, name of the exchange
            - lpTokenUnit: str, liquidity pool token unit
            - onchainID: str, on-chain identifier of the pool
            - tokenA: str, token A unit
            - tokenALocked: float, amount of token A locked in the pool
            - tokenATicker: str, token A ticker symbol
            - tokenB: str, token B unit (empty for ADA)
            - tokenBLocked: float, amount of token B locked in the pool
            - tokenBTicker: str, token B ticker symbol
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/pools"
        response = await make_get_request("Token Liquidity Pools", url, params, self.headers)
        return response


    @cache_results(table="token_prices", service_name="Token Prices")
    @rate_limit(5)
    async def get_token_prices(self, **params):
        """
        Get an object with token units (policy + hex name) as keys and price as values for a list of policies 
        and hex names. These prices are aggregated across all supported DEXs. Max batch size is 100 tokens.
        
        Args:
            body: list[str], required
                List of policy + hex names of tokens. Maximum of 100 tokens per request.
                Example: ["dda5fdb1002f7389b33e036b6afee82a8189becb6cba852e8b79b4fb0014df1047454e53"]
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary with token units as keys and prices as values.
        """
        # Extract the body parameter separately as it's required for POST
        
        
        # Filter remaining params
        body = {k: v for k, v in params.items() if v is not None}
        
        url = f"{self.base_url}/token/prices"
        
        # Using make_post_request instead of make_get_request
        response = await make_post_request("Token Prices", url, body=body["tokens"], headers=self.headers)
        return response


    @cache_results(table="token_price_percent_change", service_name="Token Price Percent Change")
    @rate_limit(5)
    async def get_token_price_percent_change(self, **params):
        """
        Get a specific token's price percent change over various timeframes. Timeframe options include 
        [5m, 1h, 4h, 6h, 24h, 7d, 30d, 60d, 90d]. All timeframes are returned by default.
        
        Args:
            unit: str, required
                Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
                Token unit (policy + hex name).
            timeframes: str, optional
                Example: timeframes=1h,4h,24h,7d,30d
                List of timeframes (comma-separated).
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary with timeframes as keys and percent changes as values.
            Example: {"1h": 0.007, "4h": -0.061, "5m": 0.024}
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/prices/chg"
        response = await make_get_request("Token Price Percent Change", url, params, self.headers)
        return response


    @cache_results(table="quote_price", service_name="Quote Price")
    @rate_limit(5)
    async def get_quote_price(self, **params):
        """
        Get current quote price (e.g, current ADA/USD price). This only returns the price of ADA against 
        the specified quote currency.
        
        Args:
            quote: str, optional
                Example: quote=USD
                Quote currency to use (USD, EUR, ETH, BTC). Default is USD.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing the current quote price.
            Example: {"price": 0.61}
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/quote"
        response = await make_get_request("Quote Price", url, params, self.headers)
        return response

    @cache_results(table="quote_currencies", service_name="Available Quote Currencies")
    @rate_limit(5)
    async def get_available_quote_currencies(self, **params):
        """
        Get all currently available quote currencies.
        
        Args:
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of available quote currencies.
            Example: ["USD", "EUR", "ETH", "BTC"]
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/quote/available"
        response = await make_get_request("Available Quote Currencies", url, params, self.headers)
        return response

    @cache_results(table="top_liquidity", service_name="Top Liquidity Tokens")
    @rate_limit(5)
    async def get_top_liquidity_tokens(self, **params):
        """
        Get tokens ranked by their DEX liquidity. This includes both AMM and order book liquidity.
        
        Args:
            page: int, optional
                Example: page=1
                This endpoint supports pagination. Default page is 1.
            perPage: int, optional
                Example: perPage=10
                Specify how many items to return per page. Maximum is 100, default is 10.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of tokens ranked by liquidity.
            Example: [
                {
                    "liquidity": 504421.3324,
                    "price": 0.537,
                    "ticker": "AGIX",
                    "unit": "b46b12f0a61721a0358988f806a7c1562e1e622d5886a73194051f336d6131"
                }
            ]
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/top/liquidity"
        response = await make_get_request("Top Liquidity Tokens", url, params, self.headers)
        return response

    @cache_results(table="top_market_cap", service_name="Top Market Cap Tokens")
    @rate_limit(5)
    async def get_top_market_cap_tokens(self, **params):
        """
        Get tokens with top market cap in a descending order. This endpoint does exclude 
        deprecated tokens (e.g. MELD V1 since there was a token migration to MELD V2).
        
        Args:
            type: str, optional
                Example: type=mcap
                Sort tokens by circulating market cap or fully diluted value. Options ["mcap", "fdv"].
            page: int, optional
                Example: page=1
                This endpoint supports pagination. Default page is 1.
            perPage: int, optional
                Example: perPage=20
                Specify how many items to return per page. Maximum is 100, default is 20.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of tokens ranked by market cap.
            Example: [
                {
                    "circSupply": 1252742236.022414,
                    "fdv": 1074222392.55,
                    "mcap": 689889366.5,
                    "price": 0.537,
                    "ticker": "AGIX",
                    "totalSupply": 1374050373.74311,
                    "unit": "b46b12f0a61721a0358988f806a7c1562e1e622d5886a73194051f336d6131"
                }
            ]
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/top/mcap"
        response = await make_get_request("Top Market Cap Tokens", url, params, self.headers)
        return response

    @cache_results(table="top_volume", service_name="Top Volume Tokens")
    @rate_limit(5)
    async def get_top_volume_tokens(self, **params):
        """
        Get tokens with top volume for a given timeframe.
        
        Args:
            timeframe: str, optional
                Example: timeframe=24h
                Specify a timeframe in which to aggregate the data by. 
                Options are ["1h", "4h", "12h", "24h", "7d", "30d", "180d", "1y", "all"]. 
                Default is 24h.
            page: int, optional
                Example: page=1
                This endpoint supports pagination. Default page is 1.
            perPage: int, optional
                Example: perPage=20
                Specify how many items to return per page. Maximum is 100, default is 20.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of tokens ranked by volume.
            Example: [
                {
                    "price": 0.537,
                    "ticker": "AGIX",
                    "unit": "b46b12f0a61721a0358988f806a7c1562e1e622d5886a73194051f336d6131",
                    "volume": 103432.3324
                }
            ]
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/top/volume"
        response = await make_get_request("Top Volume Tokens", url, params, self.headers)
        return response

    @cache_results(table="token_trades", service_name="Token Trades")
    @rate_limit(5)
    async def get_token_trades(self, **params):
        """
        Get token trades across the entire DEX market.
        
        Args:
            timeframe: str, optional
                Example: timeframe=30d
                The time interval. Options are ["1h", "4h", "24h", "7d", "30d", "90d", "180d", "1y", "all"]. 
                Defaults to 30d.
            sortBy: str, optional
                Example: sortBy=amount
                What should the results be sorted by. Options are ["amount", "time"]. 
                Default is amount. Filters to only ADA trades if set to amount.
            order: str, optional
                Example: order=desc
                Which direction should the results be sorted. Options are ["asc", "desc"]. 
                Default is desc.
            unit: str, optional
                Example: unit=279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b
                Optionally filter to a specific token by specifying a token unit (policy + hex name).
            minAmount: int, optional
                Example: minAmount=1000
                Filter to only trades of a certain ADA amount.
            from: int, optional
                Example: from=1704759422
                Filter trades using a UNIX timestamp, will only return trades after this timestamp.
            page: int, optional
                Example: page=1
                This endpoint supports pagination. Default page is 1.
            perPage: int, optional
                Example: perPage=10
                Specify how many items to return per page. Maximum is 100, default is 10.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of token trades.
            Example: [
                {
                    "action": "buy",
                    "address": "addr1q9j5jqhqak5nmqphdqt4cj9kq0gppa49afyznggw03hjzhwxr0exydkt78th5wwrjphxh0h6rrgghzwxse6q3pdf9sxqkg2mmq",
                    "exchange": "Minswap",
                    "hash": "8df1c6f66c0d02153f604ea588e792582908d3299ef6d322ae0448001791a24f",
                    "lpTokenUnit": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c35e27e3c7b4bef4824e5a4989a97e017fb8a1156d9823c20821e4d2f1fa168e4",
                    "price": 100,
                    "time": 1692781200,
                    "tokenA": "63bb8054f9142b46582198e280f489b3c928dfecb390b0cb39a5cbfe74657374746f6b656e32",
                    "tokenAAmount": 100,
                    "tokenAName": "TEST2",
                    "tokenB": "string",
                    "tokenBAmount": 200,
                    "tokenBName": "ADA"
                }
            ]
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/trades"
        response = await make_get_request("Token Trades", url, params, self.headers)
        return response

    @cache_results(table="trading_stats", service_name="Trading Stats")
    @rate_limit(5)
    async def get_trading_stats(self, **params):
        """
        Get aggregated trading stats for a particular token.
        
        Args:
            unit: str, required
                Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
                Token unit (policy + hex name)
            timeframe: str, optional
                Example: timeframe=24h
                Specify a timeframe in which to aggregate the data by. 
                Options are ["15m", "1h", "4h", "12h", "24h", "7d", "30d", "90d", "180d", "1y", "all"]. 
                Default is 24h.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing trading statistics.
            Example: {
                "buyVolume": 234123.342,
                "buyers": 134,
                "buys": 189,
                "sellVolume": 187432.654,
                "sellers": 89,
                "sells": 92
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/token/trading/stats"
        response = await make_get_request("Trading Stats", url, params, self.headers)
        return response


    # Onchain » Asset
    @cache_results(table="asset_supply", service_name="Asset Supply")
    @rate_limit(5)
    async def get_asset_supply(self, **params):
        """
        Get onchain supply for a token.
        
        Args:
            unit: str, required
                Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
                Token unit (policy + hex name) to filter by
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing the asset supply.
            Example: {
                "supply": 233838354
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/asset/supply"
        response = await make_get_request("Asset Supply", url, params, self.headers)
        return response

    # Onchain » Address
    @cache_results(table="address_info", service_name="Address Info")
    @rate_limit(5)
    async def get_address_info(self, **params):
        """
        Get address payment credential and stake address, along with its current aggregate lovelace and multi asset balance.
        Either `address` or `paymentCred` can be provided, but one must be provided.
        
        Args:
            address: str, optional
                Example: address=addr1q9j5jqhqak5nmqphdqt4cj9kq0gppa49afyznggw03hjzhwxr0exydkt78th5wwrjphxh0h6rrgghzwxse6q3pdf9sxqkg2mmq
                Address to query for
            paymentCred: str, optional
                Example: paymentCred=654902e0eda93d803768175c48b603d010f6a5ea4829a10e7c6f215d
                Payment credential to query for
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing address information.
            Example: {
                "address": "addr1q9j5jqhqak5nmqphdqt4cj9kq0gppa49afyznggw03hjzhwxr0exydkt78th5wwrjphxh0h6rrgghzwxse6q3pdf9sxqkg2mmq",
                "assets": [{}],
                "lovelace": "45000000",
                "paymentCred": "654902e0eda93d803768175c48b603d010f6a5ea4829a10e7c6f215d",
                "stakeAddress": "stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc"
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/address/info"
        response = await make_get_request("Address Info", url, params, self.headers)
        return response

    # Onchain » Address
    @cache_results(table="address_utxos", service_name="Address UTxOs")
    @rate_limit(5)
    async def get_address_utxos(self, **params):
        """
        Get current UTxOs at an address/payment credential.
        Either `address` or `paymentCred` can be provided, but one must be provided.
        
        Args:
            address: str, optional
                Example: address=addr1q9j5jqhqak5nmqphdqt4cj9kq0gppa49afyznggw03hjzhwxr0exydkt78th5wwrjphxh0h6rrgghzwxse6q3pdf9sxqkg2mmq
                Address to query for
            paymentCred: str, optional
                Example: paymentCred=654902e0eda93d803768175c48b603d010f6a5ea4829a10e7c6f215d
                Payment credential to query for
            page: int, optional
                Example: page=1
                This endpoint supports pagination. Default page is `1`.
            perPage: int, optional
                Example: perPage=100
                Specify how many items to return per page. Maximum is `100`, default is `100`.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of UTxOs at the specified address or payment credential.
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/address/utxos"
        response = await make_get_request("Address UTxOs", url, params, self.headers)
        return response

    # Onchain » Transaction
    @cache_results(table="transaction_utxos", service_name="Transaction UTxOs")
    @rate_limit(5)
    async def get_transaction_utxos(self, **params):
        """
        Get UTxOs from a specific transaction.
        
        Args:
            hash: str, required
                Example: hash=8be33680ec04da1cc98868699c5462fbbf6975529fb6371669fa735d2972d69b
                Transaction hash
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing the transaction UTxOs.
            Example: {
                "hash": "8be33680ec04da1cc98868699c5462fbbf6975529fb6371669fa735d2972d69b",
                "inputs": [{}],
                "outputs": [{}]
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/transaction/utxos"
        response = await make_get_request("Transaction UTxOs", url, params, self.headers)
        return response


    #_______________________Wallet Portfoli_________________________________________

    @cache_results(table="portfolio_positions", service_name="Portfolio Positions")
    @rate_limit(5)
    async def get_portfolio_positions(self, **params):
        """
        Get wallet's current portfolio positions with supporting market data.
        
        This includes positions that are staked in a smart contract and LP farm positions 
        (for supported protocols).
        
        Args:
            address: str, required
                Example: address=stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc
                Address to query for
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing the portfolio positions.
            Example: {
                "adaBalance": 10,
                "adaValue": 10010,
                "liquidValue": 10010,
                "numFTs": 2,
                "numNFTs": 1,
                "positionsFt": [{}],
                "positionsLp": [{}],
                "positionsNft": [{}]
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/wallet/portfolio/positions"
        response = await make_get_request("Portfolio Positions", url, params, self.headers)
        return response

    @cache_results(table="token_trade_history", service_name="Token Trade History")
    @rate_limit(5)
    async def get_token_trade_history(self, **params):
        """
        Get the token trade history for a particular wallet.
        
        Optionally pass a token unit to filter to a specific token.
        
        Args:
            address: str, required
                Example: address=stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc
                Address to query for
            unit: str, optional
                Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
                Token unit (policy + hex name) to filter by
            page: int, optional
                Example: page=1
                This endpoint supports pagination. Default page is 1.
            perPage: int, optional
                Example: perPage=100
                Specify how many items to return per page. Maximum is 100, default is 100.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of dictionaries containing the token trade history.
            Example: [{
                "action": "Buy",
                "hash": "505cb5a55f7bbe0ed70e58d97b105220ea662fb91bbd89e915ca85f07500a9b9",
                "time": 1692781200,
                "tokenA": "63bb8054f9142b46582198e280f489b3c928dfecb390b0cb39a5cbfe74657374746f6b656e32",
                "tokenAAmount": 10,
                "tokenAName": "TEST1",
                "tokenB": "string",
                "tokenBAmount": 5,
                "tokenBName": "ADA"
            }]
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/wallet/trades/tokens"
        response = await make_get_request("Token Trade History", url, params, self.headers)
        return response

    @cache_results(table="portfolio_trended_value", service_name="Portfolio Trended Value")
    @rate_limit(5)
    async def get_portfolio_trended_value(self, **params):
        """
        Get historical trended value of an address in 4hr intervals.
        
        This includes the value of all tokens, NFTs, LP/farm positions, custodial staking, 
        and assets involved in active loans (either being lent or used as collateral). 
        NOTE: this does not include staking rewards unless the rewards are withdrew from 
        rewards account.
        
        Args:
            address: str, required
                Example: address=stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc
                Address to query for
            timeframe: str, optional
                Example: timeframe=30d
                The time interval. Options are 24h, 7d, 30d, 90d, 180d, 1y, all. Defaults to 30d.
            quote: str, optional
                Example: quote=ADA
                Quote currency to use (ADA, USD, EUR, ETH, BTC). Default is ADA.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A list of dictionaries containing the portfolio trended value.
            Example: [{
                "time": 1692781200,
                "value": 57
            }]
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/wallet/value/trended"
        response = await make_get_request("Portfolio Trended Value", url, params, self.headers)
        return response



    #_________________________Integrations____________________________________

    @cache_results(table="token_details", service_name="Token Details")
    @rate_limit(5)
    async def get_token_by_id(self, **params):
        """
        Returns details of a given token by its address.
        
        Args:
            id: str, required
                Example: id=b46b12f0a61721a0358988f806a7c1562e1e622d5886a73194051f336d6131
                Token ID
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing token information.
            Example: {
                "asset": {
                    "circulatingSupply": 1500000,
                    "id": "b46b12f0a61721a0358988f806a7c1562e1e622d5886a73194051f336d6131",
                    "name": "snek coin",
                    "symbol": "SNEK",
                    "totalSupply": 2000000
                }
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/integration/asset"
        response = await make_get_request("Token Details", url, params, self.headers)
        return response


    @cache_results(table="block_info", service_name="Block Information")
    @rate_limit(5)
    async def get_block(self, **params):
        """
        Returns a specific block using either the number of the block or its timestamp.
        
        Args:
            number: int, optional
                Example: number=10937538
                Block number
            timestamp: int, optional
                Example: timestamp=1728408176
                Block timestamp
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing block information.
            Example: {
                "block": {
                    "blockNumber": 10937538,
                    "blockTimestamp": 1728408176
                }
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/integration/block"
        response = await make_get_request("Block Information", url, params, self.headers)
        return response


    @cache_results(table="block_events", service_name="Block Events")
    @rate_limit(5)
    async def get_events(self, **params):
        """
        List of events occurred in a range of blocks.
        
        Args:
            fromBlock: int, required
                Example: fromBlock=10937538
                Block number to start filtering at (inclusive).
            toBlock: int, required
                Example: toBlock=10937542
                Block number to end filtering at (inclusive).
            limit: int, optional
                Example: limit=1000
                Limit results to a maximum count, Defaults to 1000, With a maximum of 1000.
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing a list of events.
            Example: {
                "events": [
                    {}
                ]
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/integration/events"
        response = await make_get_request("Block Events", url, params, self.headers)
        return response


    @cache_results(table="dex_details", service_name="DEX Details")
    @rate_limit(5)
    async def get_dex(self, **params):
        """
        Return details of a given DEX by its factory address or alternative id.
        
        Args:
            id: str, required
                Example: id=7
                Exchange id
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing exchange information.
            Example: {
                "exchange": {
                    "factoryAddress": "3",
                    "logoURL": "https://www.logos.com/minswap.png",
                    "name": "Minswap"
                }
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/integration/exchange"
        response = await make_get_request("DEX Details", url, params, self.headers)
        return response


    @cache_results(table="latest_block", service_name="Latest Block")
    @rate_limit(5)
    async def get_latest_block(self, **params):
        """
        Returns the latest block processed in the blockchain/DEX.
        
        Args:
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing the latest block information.
            Example: {
                "block": {
                    "blockNumber": 10937538,
                    "blockTimestamp": 1728408176
                }
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/integration/latest-block"
        response = await make_get_request("Latest Block", url, params, self.headers)
        return response


    @cache_results(table="pair_details", service_name="Pair Details")
    @rate_limit(5)
    async def get_pair_by_id(self, **params):
        """
        Returns pair details (aka pool) by its address.
        
        Args:
            id: str, required
                Example: id=nikeswaporderbook.44759dc63605dbf88700b241ee451aa5b0334cf2b34094d836fbdf8642757a7a696542656520.ada
                Pair ID
            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        
        Returns:
            A dictionary containing pair information.
            Example: {
                "pair": {
                    "asset0Id": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f534e454b",
                    "asset1Id": "000000000000000000000000000000000000000000000000000000006c6f76656c616365",
                    "createdAtBlockNumber": 10937538,
                    "createdAtBlockTimestamp": 1728408176,
                    "createdAtTxnId": 115981434,
                    "factoryAddress": "4",
                    "id": "nikeswaporderbook.44759dc63605dbf88700b241ee451aa5b0334cf2b34094d836fbdf8642757a7a696542656520.ada"
                }
            }
        """
        params = {k: v for k, v in params.items() if v is not None}
        url = f"{self.base_url}/integration/pair"
        response = await make_get_request("Pair Details", url, params, self.headers)
        return response


# Add this after initializing the other API clients
taptools_api = TapToolsAPI(TAPTOOLS_API_KEY, TAPTOOLS_BASE_URL)


# Initialize API clients
blockfrost_api = BlockfrostAPI(BLOCKFROST_API_KEY, BLOCKFROST_URL)
cardanoscan_api = CardanoScanAPI(CARDANOSCAN_API_KEY, CARDANOSCAN_BASE_URL)

class CircuitBreaker:
    """Prevents cascading failures by temporarily disabling API calls after multiple timeouts"""
    
    def __init__(self, failure_threshold=5, recovery_time=300):
        self.failure_threshold = failure_threshold
        self.recovery_time = recovery_time
        self.failures = {}
        self.open_circuits = {}
    
    def record_failure(self, service_name):
        """Record a failure for a service"""
        now = time.time()
        self.failures.setdefault(service_name, [])
        
        # Add the current failure
        self.failures[service_name].append(now)
        
        # Remove failures older than recovery_time
        self.failures[service_name] = [t for t in self.failures[service_name] 
                                      if now - t < self.recovery_time]
        
        # Check if we need to open the circuit
        if len(self.failures[service_name]) >= self.failure_threshold:
            logger.warning(f"Circuit breaker tripped for {service_name}")
            self.open_circuits[service_name] = now + self.recovery_time
    
    def is_open(self, service_name):
        """Check if circuit is open for a service"""
        now = time.time()
        if service_name in self.open_circuits:
            if now < self.open_circuits[service_name]:
                return True
            else:
                # Reset the circuit if recovery time has passed
                logger.info(f"Circuit breaker reset for {service_name}")
                self.failures[service_name] = []
                del self.open_circuits[service_name]
        return False
    
    def reset(self, service_name=None):
        """Reset circuit breaker for a specific service or all services"""
        if service_name:
            if service_name in self.failures:
                del self.failures[service_name]
            if service_name in self.open_circuits:
                del self.open_circuits[service_name]
        else:
            self.failures = {}
            self.open_circuits = {}

class GracefulDegradation:
    """
    Manages application degradation when resources are unavailable,
    providing fallbacks and user-friendly messages.
    """
    
    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        self.degradation_modes = {
            "blockfrost_api": False,
            "taptools_api": False,
            "cardanoscan_api": False,
            "cache": False
        }
        self.degradation_messages = {
            "blockfrost_api": "Blockchain data access is currently limited. Some features may be unavailable.",
            "taptools_api": "Token holder information is temporarily unavailable.",
            "cardanoscan_api": "Governance data is temporarily unavailable.",
            "cache": "Performance may be reduced due to caching issues."
        }
    
    def enter_degraded_mode(self, component):
        """Enter degraded mode for a component"""
        if component in self.degradation_modes:
            self.degradation_modes[component] = True
            logger.warning(f"Entering degraded mode for {component}")
    
    def exit_degraded_mode(self, component):
        """Exit degraded mode for a component"""
        if component in self.degradation_modes:
            self.degradation_modes[component] = False
            logger.info(f"Exiting degraded mode for {component}")
    
    def is_degraded(self, component):
        """Check if a component is in degraded mode"""
        return self.degradation_modes.get(component, False)
    
    def get_degradation_message(self):
        """Get combined degradation message for all degraded components"""
        messages = [] 
        for component, is_degraded in self.degradation_modes.items():
            if is_degraded:
                messages.append(self.degradation_messages.get(component, ""))
        
        if messages:
            return "\n\n⚠️ **System Status**: " + " ".join(messages)
        return ""
    
    def handle_api_call(self, component, endpoint, fallback_data=None):
        """
        Decide whether to make an API call based on circuit breaker and degradation status.
        Return fallback data if the call should be avoided.
        """
        if self.circuit_breaker.is_open(f"{component}:{endpoint}") or self.is_degraded(component):
            logger.warning(f"Avoiding API call to {component}:{endpoint} due to circuit breaker or degraded mode")
            return fallback_data, True
        
        return None, False
    
    def record_api_failure(self, component, endpoint):
        """Record an API failure and potentially enter degraded mode"""
        self.circuit_breaker.record_failure(f"{component}:{endpoint}")
        
        # Check if we need to enter degraded mode
        if self.circuit_breaker.is_open(f"{component}:{endpoint}"):
            self.enter_degraded_mode(component)

# Helper functions for API pagination
async def fetch_all_pages(endpoint: str, params: Optional[Dict] = None, max_pages: Optional[int] = None) -> List:
    """
    Fetches all pages of data from a paginated API endpoint.
    
    Args:
        endpoint: The API endpoint to fetch from
        params: Initial parameters for the request
        max_pages: Optional maximum number of pages to fetch (if None, fetch ALL pages)
        
    Returns:
        List of all results from all pages
    """
    all_results = []
    params = params or {}
    page = 1
    
    # If not explicitly set, default count to 100 (maximum allowed)
    if 'count' not in params:
        params['count'] = 100
    
    while True:
        params['page'] = page
        response = await blockfrost_api.get(endpoint, params=params)
        
        # Check for errors
        if isinstance(response, dict) and 'error' in response:
            logger.error(f"Error fetching data: {response['error']}")
            break
        
        # Add results from this page
        if response:
            if isinstance(response, list):
                results = response
            elif isinstance(response, dict) and 'data' in response:
                results = response['data']
            else:
                results = response
                
            if isinstance(results, list):
                all_results.extend(results)
            else:
                # Single item response, add it once
                all_results.append(results)
                break
        
        # Check if we've reached the end or exceeded max_pages
        if not isinstance(response, list) or len(response) < params['count'] or (max_pages and page >= max_pages):
            break
            
        page += 1
        
        # Apply rate limiting between pages
        await asyncio.sleep(0.2)  # 5 requests per second
    
    return all_results

# ================================
# Redis Caching Implementation
# ================================


class RedisDb(redis.Redis):
    
    def __init__(self, pool: redis.ConnectionPool):
        self.pool = pool
        self.cache_available = False
        self.first_conn = True
        self.last_connection_attempt = None
        
       
    async def init_db(self, force_reconnect=False):
        """
        Initialize Redis cache_db client with improved connection handling

        Args:
            force_reconnect: Force a reconnection attempt even if recently tried

        Returns:
            Boolean indicating if initialization was successful
        """
        
        if self.last_connection_attempt is None:
            self.last_connection_attempt = 0

        # Use lock to prevent concurrent initialization attempts
        async with cache_init_lock:
            # Check if already initialized and not forcing reconnect
            if self.cache_available and not force_reconnect:
                return True

            # Check if we've recently tried to connect and failed
            current_time = time.time()
            if not force_reconnect and not self.cache_available and (current_time - self.last_connection_attempt) < CONNECTION_RETRY_INTERVAL:
                logger.debug(f"Skipping cache_db initialization - recently attempted at {self.last_connection_attempt}")
                return False

            self.last_connection_attempt = current_time

            try:
                logger.info(f"Initializing Redis cache_db client with URL: {REDIS_URL}")

                # Optional: Only disconnect if you plan to force reconnect
                if self.first_conn:
                    super().__init__(connection_pool=self.pool)
                    self.first_conn = False
                

                # Test connection
                await self.ping()

                logger.info("Successfully connected to Redis cache_db")
                self.cache_available = True
                return True

            except Exception as e:
                logger.error(f"Failed to initialize Redis cache_db: {e}")
                logger.error(traceback.format_exc())
                self.cache_available = False
                return False
            
    async def ensure_cache_connection(self):
        """
        Ensures that the cache connection is available before operations
        
        Returns:
            Boolean indicating if cache is available
        """
        if not self.cache_available:
            return await self.init_db()
        return True
    

    async def get_cached_data(
        self,
        table: str,
        key_field: str,
        key_value: str,
        order_field: Optional[str] = None,
        order_ascending: bool = False,
        limit: int = 50,
        retry_on_failure: bool = True,
        ignore_expiry: bool = False
    ) -> Optional[List]:
        """
        Retrieve cached data with improved error handling and option to ignore expiry

        Args:
            table: Cache table name
            key_field: Field name to filter by
            key_value: Value to filter for
            order_field: Optional field to order by
            order_ascending: Direction of ordering
            limit: Maximum number of records to return
            retry_on_failure: Whether to retry on connection failure
            ignore_expiry: Whether to ignore expiration dates

        Returns:
            List of cached data or None if not available
        """
        if not await self.ensure_cache_connection():
            return None

        logger.info(f"Checking cache in {table} for {key_field}={key_value}")
        try:
            limit = max(limit, -1)
            result = await self.lrange(f"{table}:{key_field}:{key_value}", 0, limit)

            flattened_result = []
            for data in result:
                flattened_result.extend(json.loads(data))
            result = flattened_result

            if order_field:
                result.sort(key=lambda x: x.get(order_field), reverse=not order_ascending)

            if result:
                now = datetime.now().isoformat()
                if ignore_expiry:
                    logger.info(f"Found {len(result)} cache entries in {table} (ignoring expiry)")
                    if isinstance(result[0],dict) and "is_single_dict" in result[0]:
                        del result[0]["is_single_dict"]
                        return result[0]
                    return result
                else:
                    valid_entries = []
                    for item in result:
                        
                        if not isinstance(item, dict):
                            valid_entries.append(item)
                            continue

                        if item.get('expires_at', now) > now:
                            if "created_at" in item: del item["created_at"]
                            if 'expires_at' in item: del item["expires_at"]
                            if "cache_key" in item : del item["cache_key"]
                            if "is_single_dict" in item:
                                del item["is_single_dict"]
                                return item
                            valid_entries.append(item)
                    if valid_entries:
                        logger.info(f"Found {len(valid_entries)} valid cache entries in {table}")
                        
                        return valid_entries
                    else:
                        logger.info(f"All cache entries have expired in {table}")
            else:
                logger.info(f"No cache found in {table} for {key_field}={key_value}")
            return None

        except Exception as e:
            logger.error(f"Error retrieving cached data from {table}: {e}")
            logger.error(traceback.format_exc())

            if retry_on_failure:
                logger.info("Attempting to reconnect to cache and retry...")
                self.cache_available = False
                if await self.ensure_cache_connection():
                    return await self.get_cached_data(
                        table, key_field, key_value,
                        order_field, order_ascending,
                        limit, False, ignore_expiry
                    )
            return None

    async def clear_cached_data(self, table: str, key_field: str, key_value: str) -> bool:
        """
        Clear existing cached data for a specific key

        Args:
            table: Cache table name
            key_field: Field name to filter by when clearing
            key_value: Value to filter for when clearing

        Returns:
            Boolean indicating success or failure
        """
        if not await self.ensure_cache_connection():
            return False

        try:
            await self.delete(f"{table}:{key_field}:{key_value}")
            logger.info(f"Cleared existing cache in {table} for {key_field}={key_value}")
            return True
        except Exception as e:
            logger.error(f"Error clearing cache in {table}: {e}")
            logger.error(traceback.format_exc())
            return False

    @create_task_decorator
    async def store_cached_data(
        self,
        table: str,
        key_field: str,
        key_value: str,
        data_items: Union[Dict, List[Dict]],
        validator_function=None,
        ttl_hours: int = 24,
        batch_size: int = 20,
        clear_existing: bool = True
    ) -> bool:
        """
        Store data in cache with improved error handling and batch processing

        Args:
            table: Cache table name
            key_field: Field name to filter by when clearing existing
            key_value: Value to filter for when clearing existing
            data_items: List of data items to cache
            validator_function: Optional function to validate items before caching
            ttl_hours: Time-to-live in hours
            batch_size: Size of batches when inserting
            clear_existing: Whether to clear existing cached data

        Returns:
            Boolean indicating success or failure
        """
        if not await self.ensure_cache_connection():
            return False

        if not data_items:
            logger.warning(f"No data items provided to cache for {table}")
            return False

        if isinstance(data_items,dict):
            data_items = dict(data_items)
            data_items["is_single_dict"] = 1
            data_items = [data_items]
        valid_items = data_items
        if validator_function:
            valid_items = [item for item in data_items if validator_function(item)]
            if len(valid_items) != len(data_items):
                logger.warning(f"Filtered out {len(data_items) - len(valid_items)} invalid entries for {table}")

        if not valid_items:
            logger.warning(f"No valid items to cache for {table} with {key_field}={key_value}")
            return False

        logger.info(f"Caching {len(valid_items)} items for {table} with {key_field}={key_value}")

        try:
            now = datetime.now()
            expires_at = (now + timedelta(hours=ttl_hours)).isoformat()

            if clear_existing:
                await self.clear_cached_data(table, key_field, key_value)

            cache_items = []
            for item in valid_items:
                if not isinstance(item,dict):
                    cache_items.append(item)
                    continue
                cache_item = item.copy()
                if key_field not in cache_item or cache_item[key_field] != key_value:
                    cache_item[key_field] = key_value
                if "created_at" not in cache_item:
                    cache_item["created_at"] = now.isoformat()
                if "expires_at" not in cache_item:
                    cache_item["expires_at"] = expires_at
                cache_items.append(cache_item)

            success = True
            total_batches = math.ceil(len(cache_items) / batch_size)

            for i in range(0, len(cache_items), batch_size):
                batch = cache_items[i:i + batch_size]
                batch_num = i // batch_size + 1

                try:
                    

                    await self.rpush(f"{table}:{key_field}:{key_value}", json.dumps(batch))
                    logger.info(f"Inserted batch {batch_num}/{total_batches} to {table}: {len(batch)} records")
                    if i + batch_size < len(cache_items):
                        await asyncio.sleep(0.3)
                except Exception as insert_error:
                    logger.error(f"Error inserting batch {batch_num}/{total_batches} to {table}: {insert_error}")
                    success = False
            if ttl_hours !=-1:
                await self.expire(f"{table}:{key_field}:{key_value}", ttl_hours * 3600)
            return success
        except Exception as e:
            logger.error(f"Error caching data to {table}: {e}")
            logger.error(traceback.format_exc())
            self.cache_available = False
            return False
        
redis_cache = RedisDb(pool=pool)
# ================================
# Supabase Caching Implementation
# ================================

async def init_supabase(force_reconnect=False):
    """
    Initialize Supabase client with improved connection handling
    
    Args:
        force_reconnect: Force a reconnection attempt even if recently tried
        
    Returns:
        Boolean indicating if initialization was successful
    """
    global supabase, cache_available, last_connection_attempt
    
    # Use lock to prevent concurrent initialization attempts
    async with cache_init_lock:
        # Check if already initialized and not forcing reconnect
        if supabase and cache_available and not force_reconnect:
            return True
            
        # Check if we've recently tried to connect and failed
        current_time = time.time()
        if not force_reconnect and not cache_available and (current_time - last_connection_attempt) < CONNECTION_RETRY_INTERVAL:
            logger.debug(f"Skipping Supabase initialization - recently attempted at {last_connection_attempt}")
            return False
            
        last_connection_attempt = current_time
        
        try:
            if SUPABASE_URL and SUPABASE_KEY:
                logger.info(f"Initializing Supabase client with URL: {SUPABASE_URL}")
                supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
                
                # Test connection with a simple query
                test_result = supabase.from_('cache_metadata').select('*').limit(1).execute()
                
                if hasattr(test_result, 'data'):
                    logger.info("Successfully connected to Supabase")
                    cache_available = True
                    return True
                else:
                    logger.warning("Supabase initialization failed: unexpected response format")
                    cache_available = False
                    return False
            else:
                logger.warning("Supabase URL or key missing, caching will be disabled")
                cache_available = False
                return False
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            logger.error(traceback.format_exc())
            cache_available = False
            return False



async def ensure_cache_connection():
    """
    Ensures that the cache connection is available before operations
    
    Returns:
        Boolean indicating if cache is available
    """
    global supbase_cache_available
    
    if not cache_available:
        return await init_supabase()
    return True


def summarize_assets(assets_list):
    """Create a concise summary of assets"""
    if not assets_list:
        return {"total": 0, "summary": "No assets found"}
    
    total = len(assets_list)
    # Group by policy ID
    policies = {}
    for asset in assets_list:
        unit = asset.get('unit', '')
        policy_id = unit[:56] if len(unit) > 56 else unit
        if policy_id in policies:
            policies[policy_id]["count"] += 1
        else:
            policies[policy_id] = {"count": 1, "sample": asset}
    
    # Create summary with top 5 policies
    top_policies = sorted(policies.items(), key=lambda x: x[1]["count"], reverse=True)[:5]
    policy_summaries = [
        {"policy_id": pid[:8] + "...", "asset_count": info["count"]}
        for pid, info in top_policies
    ]
    
    return {
        "total_assets": total,
        "unique_policies": len(policies),
        "top_policies": policy_summaries,
        "has_more": len(policies) > 5
    }

def format_pagination_info(page_data):
    """Format pagination information for response text"""
    pagination_text = f"\n## Pagination Information\n"
    pagination_text += f"- Current Page: {page_data['page']} of {page_data['total_pages']}\n"
    pagination_text += f"- Total Items: {page_data['total_items']}\n"
    pagination_text += f"- Items Per Page: {page_data['items_per_page']}\n"
    
    if page_data['page'] < page_data['total_pages']:
        pagination_text += f"- For next page, use page={page_data['page'] + 1}\n"
    if page_data['page'] > 1:
        pagination_text += f"- For previous page, use page={page_data['page'] - 1}\n"
    
    return pagination_text

def chunk_text(text, max_chunk_size=4000):
    """Split text into chunks of manageable size"""
    if len(text) <= max_chunk_size:
        return [text]
    
    chunks = []
    current_chunk = ""
    
    # Split by paragraphs
    paragraphs = text.split("\n\n")
    
    for paragraph in paragraphs:
        # If adding this paragraph would exceed chunk size, save current chunk and start new one
        if len(current_chunk) + len(paragraph) + 2 > max_chunk_size:
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = paragraph
        else:
            if current_chunk:
                current_chunk += "\n\n"
            current_chunk += paragraph
    
    # Add the last chunk if it's not empty
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks

def filter_asset_data(asset_data, include_fields=None, exclude_fields=None):
    """Filter asset data to include/exclude specific fields"""
    if not asset_data:
        return asset_data
    
    if not include_fields and not exclude_fields:
        return asset_data
    
    result = []
    for asset in asset_data:
        if isinstance(asset, dict):
            filtered_asset = {}
            for key, value in asset.items():
                if include_fields and key not in include_fields:
                    continue
                if exclude_fields and key in exclude_fields:
                    continue
                filtered_asset[key] = value
            result.append(filtered_asset)
        else:
            result.append(asset)
    
    return result

async def cache_single_item(table: str, item: Dict, ttl_hours: int = 24, 
                           upsert: bool = True) -> bool:
    """
    Cache a single item with improved error handling
    
    Args:
        table: Cache table name
        item: Item to cache
        ttl_hours: Time-to-live in hours
        upsert: Whether to use upsert (update or insert) instead of insert
        
    Returns:
        Boolean indicating success or failure
    """
    global supabase, cache_available
    
    # Ensure cache connection
    if not await ensure_cache_connection():
        return False
    
    if not item:
        logger.warning(f"No item provided to cache in {table}")
        return False
    
    logger.info(f"Caching single item in {table}")
    
    try:
        # Set expiration time
        now = datetime.now()
        expires_at = (now + timedelta(hours=ttl_hours)).isoformat()
        
        # Add timestamps
        cache_item = item.copy()
        if "created_at" not in cache_item:
            cache_item["created_at"] = now.isoformat()
        if "expires_at" not in cache_item:
            cache_item["expires_at"] = expires_at
        
        # Upsert or insert to cache
        if upsert:
            result = supabase.from_(table).upsert([cache_item]).execute()
        else:
            result = supabase.from_(table).insert([cache_item]).execute()
            
        logger.info(f"Successfully cached item in {table}")
        return True
    except Exception as e:
        logger.error(f"Error caching item in {table}: {e}")
        logger.error(traceback.format_exc())
        return False

async def update_cached_item(table: str, key_field: str, key_value: str, 
                           updates: Dict, ttl_hours: Optional[int] = None) -> bool:
    """
    Update specific fields of cached items
    
    Args:
        table: Cache table name
        key_field: Field name to filter by
        key_value: Value to filter for
        updates: Dictionary of field updates to apply
        ttl_hours: Optional new TTL in hours
        
    Returns:
        Boolean indicating success or failure
    """
    global supabase, cache_available
    
    # Ensure cache connection
    if not await ensure_cache_connection():
        return False
    
    try:
        update_data = updates.copy()
        
        # Update expiration if requested
        if ttl_hours is not None:
            expires_at = (datetime.now() + timedelta(hours=ttl_hours)).isoformat()
            update_data["expires_at"] = expires_at
        
        # Apply updates
        result = supabase.from_(table).update(update_data).eq(key_field, key_value).execute()
        logger.info(f"Updated cache in {table} for {key_field}={key_value}")
        return True
    except Exception as e:
        logger.error(f"Error updating cache in {table}: {e}")
        logger.error(traceback.format_exc())
        return False

# Validation functions

# Validator function for active loans data
def validate_active_loan_data(loan_data: Dict) -> bool:
    """Validate active loan data before caching"""
    required_fields = ['hash', 'protocol', 'time', 'expiration']
    for field in required_fields:
        if field not in loan_data:
            return False
    
    # Validate numeric fields
    numeric_fields = ['collateralAmount', 'collateralValue', 'debtAmount', 
                      'debtValue', 'interestAmount', 'interestValue', 'health']
    for field in numeric_fields:
        if field in loan_data and not isinstance(loan_data[field], (int, float, str)):
            # If it's a string, try to convert it to float
            if isinstance(loan_data[field], str):
                try:
                    float(loan_data[field])
                except ValueError:
                    return False
            else:
                return False
    
    return True

def validate_data(data: Any, required_fields: List[str] = None, field_types: Dict[str, type] = None) -> bool:
    """
    Generic validation function for data items
    
    Args:
        data: Data item to validate
        required_fields: List of required field names
        field_types: Dictionary mapping field names to their expected types
        
    Returns:
        Boolean indicating if the data is valid
    """
    # Check if data is a dictionary
    if not isinstance(data, dict):
        return False
    
    # Check required fields
    if required_fields:
        if not all(field in data for field in required_fields):
            return False
    
    # Check field types
    if field_types:
        for field, expected_type in field_types.items():
            if field in data and not isinstance(data[field], expected_type):
                # Special case for numeric fields that might be strings
                if expected_type in (int, float) and isinstance(data[field], str):
                    try:
                        float(data[field])  # This will raise ValueError if not convertible
                    except ValueError:
                        return False
                else:
                    return False
    
    return True

# Validator function for token indicators data
def validate_token_indicators_data(indicator_data: Any) -> bool:
    """Validate token indicator data before caching"""
    # For token indicators, we expect either a list of numbers or an object with specific properties
    if isinstance(indicator_data, list):
        # Simple array of indicator values
       
       return all(isinstance(obj, dict) for obj in indicator_data)
        # return all(isinstance(value, (int, float)) or 
        #           (isinstance(value, str) and value.replace('.', '', 1).isdigit()) 
        #           for value in indicator_data)
    elif isinstance(indicator_data, dict):
        # More complex indicators like MACD return objects
        # Basic validation for common fields
        return True  # For now, accept any dict structure
    else:
        return False

# Add this validation function with other validation functions
def validate_loan_offer_data(offer_data: Dict) -> bool:
    """Validate loan offer data before caching"""
    required_fields = ['hash', 'protocol', 'time']
    for field in required_fields:
        if field not in offer_data:
            return False
    
    # Validate numeric fields
    numeric_fields = ['collateralAmount', 'collateralValue', 'debtAmount', 
                      'debtValue', 'interestAmount', 'interestValue', 'health']
    for field in numeric_fields:
        if field in offer_data and not isinstance(offer_data[field], (int, float, str)):
            # If it's a string, try to convert it to float
            if isinstance(offer_data[field], str):
                try:
                    float(offer_data[field])
                except ValueError:
                    return False
            else:
                return False
    
    return True

# Add this validation function with the others
def validate_token_holders_data(holder_data: Dict) -> bool:
    """Validate token holder data before caching"""
    return validate_data(
        holder_data,
        required_fields=['address', 'amount'],
        field_types={
            'address': str,
            'amount': (int, float)
        }
    )

def validate_loan_offer_data(offer_data: Dict) -> bool:
    """Validate loan offer data before caching"""
    required_fields = ['hash', 'protocol', 'time']
    for field in required_fields:
        if field not in offer_data:
            return False
    
    # Validate numeric fields
    numeric_fields = ['collateralAmount', 'collateralValue', 'debtAmount', 
                      'debtValue', 'interestAmount', 'interestValue', 'health']
    for field in numeric_fields:
        if field in offer_data and not isinstance(offer_data[field], (int, float, str)):
            # If it's a string, try to convert it to float
            if isinstance(offer_data[field], str):
                try:
                    float(offer_data[field])
                except ValueError:
                    return False
            else:
                return False
    
    return True



# Add this function to create a formatted token guide
def create_token_guide(token_lookup: Dict[str, Dict]) -> str:
    """
    Create a formatted token guide from token lookup data.
    
    Args:
        token_lookup: Dictionary of token data
        
    Returns:
        Formatted string with token guide
    """
    if not token_lookup:
        return "No token data available."
    
    # Group tokens by ticker for better organization
    tokens_by_ticker = {}
    for token_id, token_data in token_lookup.items():
        ticker = token_data.get('ticker', '').upper()
        if not ticker:
            continue
            
        if ticker not in tokens_by_ticker:
            tokens_by_ticker[ticker] = token_data
    
    # Format the guide
    guide_lines = ["# Cardano Token Guide", ""]
    guide_lines.append("This guide contains the most popular tokens on Cardano, organized by ticker:")
    guide_lines.append("")
    
    # Add the most common tokens
    guide_lines.append("## Common Tokens")
    guide_lines.append("| Ticker | Name | Policy ID | Unit (for API) |")
    guide_lines.append("|--------|------|-----------|----------------|")
    
    # Sort tickers alphabetically
    sorted_tickers = sorted(tokens_by_ticker.keys())
    
    # Get the 20 most important tokens (this could be refined with more logic)
    important_tokens = sorted_tickers[:20]
    
    for ticker in important_tokens:
        token_data = tokens_by_ticker[ticker]
        name = token_data.get('name', 'Unknown')
        policy = token_data.get('policy', '')
        full_unit = token_data.get('full_unit', '')
        
        guide_lines.append(f"| {ticker} | {name} | {policy[:8]}... | {full_unit[:8]}... |")
    
    guide_lines.append("")
    guide_lines.append("When using the token_holders_analysis tool, always use the full Unit value.")
    
    return "\n".join(guide_lines)

# Add this helper function to get token info
async def get_token_info(policy_id: str) -> Optional[Dict]:
    """
    Get token information from cexplorer_assets table.
    
    Args:
        policy_id: The policy ID to look up
        
    Returns:
        Token information or None if not found
    """
    if not await ensure_cache_connection():
        return None
    
    try:
        query = supabase.from_('cexplorer_assets').select('*').eq('policy', policy_id)
        result = query.execute()  # Don't await this
        
        if result.data and len(result.data) > 0:
            return result.data[0]
        return None
    except Exception as e:
        logger.error(f"Error fetching token info: {e}")
        logger.error(traceback.format_exc())
        return None

# Add this helper function to prepare cexplorer assets for context
async def get_assets_context() -> str:
    """
    Get assets from cexplorer_assets table formatted as context.
    
    Returns:
        Formatted assets context string
    """
    if not await ensure_cache_connection():
        return "Token lookup database not available."
    
    try:
        # Get all tokens from the database (no limit)
        result = await supabase.from_('cexplorer_assets').select('*').execute()
        
        if not result.data or len(result.data) == 0:
            return "No token data available."
        
        # Format the assets context
        context = "# Token Reference\n\n"
        context += f"Database contains {len(result.data)} tokens. When analyzing tokens, you can use any of these verified token identifiers.\n\n"
        context += "Common tokens include:\n\n"
        context += "| Token Name | Ticker | Policy ID | Full Unit |\n"
        context += "|------------|--------|-----------|----------|\n"
        
        # Show the top tokens by quantity (first 15)
        sorted_tokens = sorted(result.data, key=lambda x: x.get('quantity', 0) or 0, reverse=True)
        for token in sorted_tokens[:15]:
            name = token.get('registry_name') or token.get('name_small') or 'Unknown'
            ticker = token.get('registry_ticker', '')
            policy = token.get('policy', '')
            name = token.get('name', '')
            full_unit = f"{policy}{name}"
            
            context += f"| {name} | {ticker} | {policy[:8]}... | {full_unit[:8]}... |\n"
        
        context += "\nThe database contains many more tokens not listed above."
        context += "\nWhen using token analysis tools, you can use the token name, ticker, or full unit."
        
        return context
    except Exception as e:
        logger.error(f"Error preparing assets context: {e}")
        logger.error(traceback.format_exc())
        return "Error retrieving token database."

def validate_reward_data(reward: Dict) -> bool:
    """Validate reward data before caching"""
    return validate_data(
        reward,
        required_fields=['epoch', 'amount', 'pool_id'],
        field_types={
            'epoch': (int, float),
            'amount': (int, float, str),
            'pool_id': str
        }
    )

def validate_delegation_data(delegation: Dict) -> bool:
    """Validate delegation data before caching"""
    return validate_data(
        delegation,
        required_fields=['active_epoch', 'tx_hash', 'pool_id'],
        field_types={
            'active_epoch': (int, float),
            'tx_hash': str,
            'pool_id': str
        }
    )

def validate_registration_data(registration: Dict) -> bool:
    """Validate registration data before caching"""
    return validate_data(
        registration,
        required_fields=['tx_hash', 'action'],
        field_types={
            'tx_hash': str,
            'action': str
        }
    )

def validate_withdrawal_data(withdrawal: Dict) -> bool:
    """Validate withdrawal data before caching"""
    return validate_data(
        withdrawal,
        required_fields=['tx_hash', 'amount'],
        field_types={
            'tx_hash': str,
            'amount': (int, float, str)
        }
    )


def validate_address_data(address_info: Dict) -> bool:
    """Validate address data before caching"""
    return validate_data(
        address_info,
        required_fields=['address'],
        field_types={
            'address': str
        }
    )

def validate_utxo_data(utxo: Dict) -> bool:
    """Validate UTXO data before caching"""
    return validate_data(
        utxo,
        required_fields=['tx_hash', 'output_index'],
        field_types={
            'tx_hash': str,
            'output_index': (int, float, str)
        }
    )

def validate_transaction_data(tx_data: Dict) -> bool:
    """Validate transaction data before caching"""
    return validate_data(
        tx_data,
        required_fields=['tx_hash'],
        field_types={
            'tx_hash': str
        }
    )

def validate_asset_data(asset_data: Dict) -> bool:
    """Validate asset data before caching"""
    return validate_data(
        asset_data,
        required_fields=['unit'],
        field_types={
            'unit': str,
            'quantity': (int, float, str)
        }
    )

async def get_asset_database() -> List[Dict]:
    """
    Retrieves the complete asset database from cexplorer_assets table.
    
    Returns:
        List of asset entries or empty list if not available
    """
    if not await ensure_cache_connection():
        logger.warning("Cache not available for asset database")
        return []
    
    try:
        # Retrieve all assets from the cexplorer_assets table
        # Remove the await - it's causing the error
        result = supabase.from_('cexplorer_assets').select('*').execute()
        
        if result.data:
            return result.data
        
        return []
    except Exception as e:
        logger.error(f"Error retrieving asset database: {e}")
        logger.error(traceback.format_exc())
        return []


# Helper function for fetch-and-cache pattern
async def fetch_and_cache_data(
    endpoint: str, 
    cache_table: str, 
    key_field: str, 
    key_value: str, 
    validator: callable = None,
    order_field: str = None,
    params: Dict = None,
    ttl_hours: int = 24,
    timeout: float = 30.0,
    use_expired_cache: bool = True
) -> List:
    """
    Fetch data from API and cache it, or return from cache if available.
    Handles timeouts gracefully and can fall back to expired cache data.
    
    Args:
        endpoint: API endpoint to fetch data from
        cache_table: Cache table name
        key_field: Field name to identify the data
        key_value: Value for the key field
        validator: Validation function for data items
        order_field: Field to order results by
        params: Additional API parameters
        ttl_hours: Cache TTL in hours
        timeout: API request timeout in seconds
        use_expired_cache: Whether to use expired cache data as fallback
        
    Returns:
        List of data items or empty list/error indicator on failure
    """
    # Check if data is in cache (active/non-expired)
    global redis_cache
    cached_data = await redis_cache.get_cached_data(
        cache_table, key_field, key_value, 
        order_field=order_field,
        ignore_expiry=False
    )
    
    if cached_data:
        logger.info(f"Using active cached data from {cache_table} for {key_field}={key_value}")
        return cached_data
    
    # Try to get expired cache data as backup
    expired_data = None
    if use_expired_cache:
        expired_data = await redis_cache.get_cached_data(
            cache_table, key_field, key_value, 
            order_field=order_field,
            ignore_expiry=True
        )
        if expired_data:
            logger.info(f"Found expired cache data from {cache_table} for {key_field}={key_value} (will be used as fallback)")
    
    # Fetch fresh data from API with timeout handling
    logger.info(f"Fetching fresh data from {endpoint} for {key_field}={key_value}")
    api_data = await blockfrost_api.get(endpoint, params=params, timeout=timeout)
    
    # Check for API errors
    if isinstance(api_data, dict) and 'error' in api_data:
        logger.error(f"API error for {endpoint}: {api_data['error']}")
        
        # If timeout or other retriable error, use expired data as fallback
        if use_expired_cache and expired_data and (
            api_data.get('error') == 'API timeout' or 
            'timeout' in api_data.get('details', '').lower()
        ):
            logger.warning(f"Using expired cache as fallback due to API timeout")
            
            # Mark the data as being from expired cache
            for item in expired_data:
                if isinstance(item, dict):
                    item['_from_expired_cache'] = True
            
            return expired_data
        
        # Return empty list with error indicator if no fallback
        return [{"error": api_data['error'], "details": api_data.get('details', '')}]
    
    # Ensure we have a list of data items
    data_items = api_data if isinstance(api_data, list) else [api_data]
    
    # Cache the data
    if data_items:
        redis_cache.store_cached_data(
            cache_table, key_field, key_value,
            data_items, validator_function=validator,
            ttl_hours=ttl_hours
        )
    
    return data_items



#______________________________________syncai class_____________________________________________



class SyncAIAPI:
    def __init__(self):
        self.base_url = SYNCAI_BASE_URL
        self.headers = {
            "x-api-key": SYNCAI_KEY,
            "Content-Type": "application/json"
        }

    @cache_results(table="proposal_access", service_name="Proposal Access")
    @rate_limit(5)
    async def check_proposal_access(self, proposal_id: Union[str, List[str]], **params):
        """
        Check access to proposal(s) by ID.
        """
        url = f"{self.base_url}/api/check-access"
        
        request_body = {"id": proposal_id}
        
        if params:
            request_body.update(params)
        
        access_info = await make_post_request("Proposal Access", url, request_body, self.headers)
        return access_info

    @cache_results(table="drep_search", service_name="DRep AI Search")
    @rate_limit(10)
    async def search_dreps(self, query: str, limit: int = 10,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), **params):
        """
        Search for DReps using natural language queries.
        """
        url = f"{self.base_url}/api/drep/ai"
        
        request_body = {
            "query": query,
            "limit": limit,
            "page": page
        }
        
        if params:
            request_body.update(params)
        
        drep_results = await make_post_request("DRep AI Search", url, request_body, self.headers)
        return drep_results

    @cache_results(table="spo_search", service_name="SPO AI Search")
    @rate_limit(10)
    async def search_spos(self, query: str, limit: int = 10,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), **params):
        """
        Search for SPOs (Stake Pool Operators) using natural language queries.
        """
        url = f"{self.base_url}/api/spo/ai"
        
        request_body = {
            "query": query,
            "limit": limit,
            "page": page
        }
        
        if params:
            request_body.update(params)
        
        spo_results = await make_post_request("SPO AI Search", url, request_body, self.headers)
        return spo_results

    @cache_results(table="cc_search", service_name="CC AI Search")
    @rate_limit(10)
    async def search_constitutional_committee(self, query: str, limit: int = 10,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), **params):
        """
        Search for Constitutional Committee (CC) members using natural language queries.
        """
        url = f"{self.base_url}/api/cc/ai"
        
        request_body = {
            "query": query,
            "limit": limit,
            "page": page
        }
        
        if params:
            request_body.update(params)
        
        cc_results = await make_post_request("CC AI Search", url, request_body, self.headers)
        return cc_results

    @cache_results(table="proposal_search", service_name="Advanced Proposal Search")
    @rate_limit(10)
    async def search_proposals(self, query: str, limit: int = 10,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), **params):
        """
        Search for blockchain governance proposals using natural language queries.
        """
        url = f"{self.base_url}/api/proposal/ai"
        
        request_body = {
            "query": query,
            "limit": limit,
            "page": page
        }
        
        if params:
            request_body.update(params)
        
        proposal_results = await make_post_request("Advanced Proposal Search", url, request_body, self.headers)
        return proposal_results

    @cache_results(table="proposal_analysis", service_name="AI Proposal Analysis")
    @rate_limit(5)
    async def analyze_proposal(self, query: str, govaction_id: str = None, **params):
        """
        Generate AI analysis of blockchain governance proposals and topics.
        """
        url = f"{self.base_url}/api/chat/proposal/ai"
        
        request_body = {"query": query}
        
        if govaction_id:
            request_body["govaction_id"] = govaction_id
        
        if params:
            request_body.update(params)
        
        analysis_result = await make_post_request("AI Proposal Analysis", url, request_body, self.headers)
        return analysis_result

    @cache_results(table="rationale_analysis", service_name="AI Rationale Analysis")
    @rate_limit(5)
    async def analyze_voter_rationale(self, govaction_id: str, vote_type: str, for_role: str = "all", custom_system_prompt: str = None, **params):
        """
        Analyze voter rationales for governance proposals.
        """
        url = f"{self.base_url}/api/chat/rationale/ai"
        
        request_body = {
            "govaction_id": govaction_id,
            "for": for_role,
            "vote_type": vote_type
        }
        
        if custom_system_prompt:
            request_body["custom_system_prompt"] = custom_system_prompt
        
        if params:
            request_body.update(params)
        
        rationale_analysis = await make_post_request("AI Rationale Analysis", url, request_body, self.headers)
        return rationale_analysis

    @cache_results(table="governance_search", service_name="Governance Knowledge Search")
    @rate_limit(10)
    async def search_governance_knowledge(self, query: str, **params):
        """
        Search Cardano governance documents and get AI analysis.
        """
        url = f"{self.base_url}/api/chat/governance/ai"
        
        request_body = {"query": query}
        
        if params:
            request_body.update(params)
        
        governance_results = await make_post_request("Governance Knowledge Search", url, request_body, self.headers)
        return governance_results

    @cache_results(table="trend_data", service_name="Trend Data")
    @rate_limit(15)
    async def get_trend_data(self, trend_type: str = "all", **params):
        """
        Get growth trend data for DReps and stake pools over time.
        """
        url = f"{self.base_url}/api/trend"
        
        request_body = {"type": trend_type}
        
        if params:
            request_body.update(params)
        
        trend_data = await make_post_request("Trend Data", url, request_body, self.headers)
        return trend_data

    @cache_results(table="country_distribution", service_name="Country Distribution")
    @rate_limit(20)
    async def get_drep_country_distribution(self, **params):
        """
        Get geographic distribution of DReps by country.
        """
        url = f"{self.base_url}/api/countries"
        
        country_data = await make_get_request("Country Distribution", url,{},headers=self.headers)
        return country_data

    @cache_results(table="proposal_voting_history", service_name="Proposal Voting History")
    @rate_limit(10)
    async def get_proposal_voting_history(self, gov_id: str, role: str,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), limit: int = 10, **params):
        """
        Get detailed voting history for a specific proposal by voter role.
        """
        url = f"{self.base_url}/api/proposal/voting-history"
        
        request_body = {
            "gov_id": gov_id,
            "role": role,
            "page": page,
            "limit": limit
        }
        
        if params:
            request_body.update(params)
        
        voting_history = await make_post_request("Proposal Voting History", url, request_body, self.headers)
        return voting_history

    @cache_results(table="user_voting_history", service_name="User Voting History")
    @rate_limit(10)
    async def get_user_voting_history(self, user_id: Union[str, List[str]],  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), limit: int = 10, **params):
        """
        Get voting history for specific users across all proposals.
        """
        url = f"{self.base_url}/api/user/voting-history"
        
        request_body = {
            "id": user_id,
            "page": page,
            "limit": limit
        }
        
        if params:
            request_body.update(params)
        
        user_voting_history = await make_post_request("User Voting History", url, request_body, self.headers)
        return user_voting_history

    @cache_results(table="proposal_rationales", service_name="Proposal Rationales")
    @rate_limit(10)
    async def get_proposal_rationales(self, gov_id: str, role: str = "all",  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), limit: int = 50, **params):
        """
        Get all user rationales for a specific proposal, filterable by role.
        """
        url = f"{self.base_url}/api/proposal/rationale"
        
        request_body = {
            "gov_id": gov_id,
            "role": role,
            "page": page,
            "limit": limit
        }
        
        if params:
            request_body.update(params)
        
        proposal_rationales = await make_post_request("Proposal Rationales", url, request_body, self.headers)
        return proposal_rationales

    @cache_results(table="user_rationales", service_name="User Rationales")
    @rate_limit(10)
    async def get_user_rationales(self, user_id: str,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), limit: int = 50, **params):
        """
        Get all rationales from a specific user across all proposals.
        """
        url = f"{self.base_url}/api/user/rationale"
        
        request_body = {
            "id": user_id,
            "page": page,
            "limit": limit
        }
        
        if params:
            request_body.update(params)
        
        user_rationales = await make_post_request("User Rationales", url, request_body, self.headers)
        return user_rationales

    @cache_results(table="ada_power_history", service_name="ADA Power History")
    @rate_limit(10)
    async def get_ada_power_history(self, entity_id: Union[str, List[str]], **params):
        """
        Get historical ADA power data for pools and DReps over time.
        """
        url = f"{self.base_url}/api/ada-history"
        
        request_body = {"id": entity_id}
        
        if params:
            request_body.update(params)
        
        ada_history = await make_post_request("ADA Power History", url, request_body, self.headers)
        return ada_history



syncai_api = SyncAIAPI()


#____________________________________utility mcp tools_________________________________
@mcp.tool(
    name="health_check",
    description="""Check the health status of CANDI's MCP server and all integrated services.

    Use this tool to:
    - Verify server is operational
    - Check connection status to external APIs (Blockfrost, CardanoScan, TapTools, SyncAI)
    - Monitor cache availability (Supabase, Redis)
    - Get system statistics and uptime
    - Troubleshoot connection issues

    Returns comprehensive health report with service statuses.

    Example Response:
    {
        "status": "healthy",
        "timestamp": "2025-11-29T10:30:00Z",
        "services": {
            "blockfrost": "connected",
            "cardanoscan": "connected",
            "taptools": "connected",
            "syncai": "connected",
            "supabase_cache": "connected",
            "redis_cache": "connected"
        },
        "tool_count": 86,
        "uptime_seconds": 3600
    }
    """
)
async def health_check() -> str:
    """
    Comprehensive health check for CANDI MCP server.

    Returns:
        Formatted health status report as string
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "server": "CANDI MCP Server",
        "version": "1.0.0",
        "tool_count": 86,
        "services": {},
        "cache": {},
        "issues": []
    }

    # Check Blockfrost API
    try:
        if BLOCKFROST_API_KEY:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BLOCKFROST_URL}/health",
                    headers={"project_id": BLOCKFROST_API_KEY},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    health_status["services"]["blockfrost"] = "connected" if response.status == 200 else "degraded"
        else:
            health_status["services"]["blockfrost"] = "not_configured"
            health_status["issues"].append("Blockfrost API key not configured")
    except Exception as e:
        health_status["services"]["blockfrost"] = "disconnected"
        health_status["issues"].append(f"Blockfrost: {str(e)}")
        health_status["status"] = "degraded"

    # Check CardanoScan API
    try:
        if CARDANOSCAN_API_KEY:
            health_status["services"]["cardanoscan"] = "configured"
        else:
            health_status["services"]["cardanoscan"] = "not_configured"
            health_status["issues"].append("CardanoScan API key not configured")
    except Exception as e:
        health_status["services"]["cardanoscan"] = "error"
        health_status["issues"].append(f"CardanoScan: {str(e)}")

    # Check TapTools API
    try:
        if TAPTOOLS_API_KEY:
            health_status["services"]["taptools"] = "configured"
        else:
            health_status["services"]["taptools"] = "not_configured"
            health_status["issues"].append("TapTools API key not configured")
    except Exception as e:
        health_status["services"]["taptools"] = "error"
        health_status["issues"].append(f"TapTools: {str(e)}")

    # Check SyncAI API
    try:
        if SYNCAI_KEY:
            health_status["services"]["syncai"] = "configured"
        else:
            health_status["services"]["syncai"] = "not_configured"
            health_status["issues"].append("SyncAI API key not configured")
    except Exception as e:
        health_status["services"]["syncai"] = "error"
        health_status["issues"].append(f"SyncAI: {str(e)}")

    # Check Supabase Cache
    try:
        if supabase and cache_available:
            health_status["cache"]["supabase"] = "connected"
        elif SUPABASE_URL and SUPABASE_KEY:
            health_status["cache"]["supabase"] = "configured_not_connected"
        else:
            health_status["cache"]["supabase"] = "not_configured"
    except Exception as e:
        health_status["cache"]["supabase"] = "error"
        health_status["issues"].append(f"Supabase: {str(e)}")

    # Check Redis Cache
    try:
        if cache_db:
            # Try a simple ping
            await cache_db.ping()
            health_status["cache"]["redis"] = "connected"
        elif REDIS_URL and REDIS_PASSWORD:
            health_status["cache"]["redis"] = "configured_not_connected"
        else:
            health_status["cache"]["redis"] = "not_configured"
    except Exception as e:
        health_status["cache"]["redis"] = "error"
        health_status["issues"].append(f"Redis: {str(e)}")

    # Determine overall status
    if len(health_status["issues"]) > 5:
        health_status["status"] = "unhealthy"
    elif len(health_status["issues"]) > 0:
        health_status["status"] = "degraded"

    # Format as readable text
    result = f"""
🏥 CANDI MCP Server Health Check
{'='*60}
Status: {health_status['status'].upper()}
Timestamp: {health_status['timestamp']}
Server: {health_status['server']} v{health_status['version']}
Tools Available: {health_status['tool_count']}

📡 External Services:
"""
    for service, status in health_status["services"].items():
        emoji = "✅" if status in ["connected", "configured"] else "⚠️" if status == "degraded" else "❌"
        result += f"  {emoji} {service}: {status}\n"

    result += "\n💾 Cache Services:\n"
    for cache, status in health_status["cache"].items():
        emoji = "✅" if status == "connected" else "⚠️" if "configured" in status else "❌"
        result += f"  {emoji} {cache}: {status}\n"

    if health_status["issues"]:
        result += f"\n⚠️ Issues Detected ({len(health_status['issues'])}):\n"
        for issue in health_status["issues"]:
            result += f"  - {issue}\n"
    else:
        result += "\n✅ No issues detected - All systems operational!\n"

    result += "="*60

    return result


#____________________________________syncai mcp tools_________________________________
@mcp.tool(name="check_proposal_access", description="Check access to proposal(s) by ID.")
@format_results_decorator
async def check_proposal_access(
    proposal_id: Union[str, List[str]] = Field(
        ...,
        description="The proposal ID(s) to check access for. Can be a single ID, list of IDs, or comma-separated string.",
        examples=["gov_action123", ["gov_action123", "net limit"], "gov_action123, net limit, treasury proposal"]
    )
):
    return await syncai_api.check_proposal_access(proposal_id=proposal_id)

@mcp.tool(
    name="search_dreps",
    description="""Search for DReps (Delegated Representatives) using natural language queries.

    Use this tool when users ask about:
    - Finding DReps by name, voting power, or participation
    - DRep voting history on specific proposals
    - Top DReps by various metrics (voting power, participation rate, etc.)
    - DReps who voted specific ways on proposals

    Example Queries:
    - "What are the top 5 DReps by voting power?"
    - "Find DReps who voted yes on the treasury withdrawal proposal"
    - "Show me the voting history of DRep Yuta"
    - "Which DReps have over 100M ADA voting power?"

    Returns: Paginated list of DReps with voting data, power, and participation stats.
    """
)
async def search_dreps(
    query: str = Field(
        ...,
        description="Natural language query about DReps",
        examples=[
            "give me voting history of drep yuta",
            "Find DReps who voted yes on the net change limit",
            "find dreps with voting power more than 100k"
        ]
    ),
    limit: int = Field(
        default=10,
        description="Number of results to return",
        ge=1,
        le=100
    ),
    page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    )
):
    data, err, component = await syncai_api.search_dreps(
        query=query,
        limit=limit,
        page=page
    )
    if err:
        return f"Could not search DReps, err: {err}"
    
    main_key = "data"
    result_text = f"DRep Search Results\n"
    result_text += format_results(data.get(main_key, []))
    return result_text

@mcp.tool(
    name="search_spos",
    description="""Search for SPOs (Stake Pool Operators) using natural language queries.

    Use this tool when users ask about:
    - Finding stake pools by name, ticker, or voting behavior
    - SPO voting history on governance proposals
    - Top SPOs by voting power or participation
    - Pools that voted specific ways on proposals

    Example Queries:
    - "What are the top 10 SPOs by voting power?"
    - "Find SPOs who voted yes on the network parameter change"
    - "Show me the voting history of pool TERA3"
    - "Which pools have the highest participation rate?"

    Returns: Paginated list of SPOs with voting history, power, and pool information.
    """
)
async def search_spos(
    query: str = Field(
        ...,
        description="Natural language query about SPOs (Stake Pool Operators)",
        examples=[
            "give me voting history of spo TERA3",
            "Find spos who voted yes on cardano net change",
            "spo voted no on treasury proposals"
        ]
    ),
    limit: int = Field(
        default=10,
        description="Number of results to return",
        ge=1,
        le=100
    ),
    page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    )
):
    data, err, component = await syncai_api.search_spos(
        query=query,
        limit=limit,
        page=page
    )
    if err:
        return f"Could not search SPOs, err: {err}"
    
    main_key = "data"
    result_text = f"SPO Search Results\n"
    result_text += format_results(data.get(main_key, []))
    return result_text

@mcp.tool(name="search_constitutional_committee", description="Search for Constitutional Committee members using natural language queries.")
async def search_constitutional_committee(
    query: str = Field(
        ...,
        description="Natural language query about Constitutional Committee members",
        examples=[
            "give me voting history of Cardano Atlantic",
            "Find CC members who voted yes on governance actions",
            "which committee has single members"
        ]
    ),
    limit: int = Field(
        default=10,
        description="Number of results to return default 10",
        ge=1,
        le=100
    ),
    page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    )
):
    data, err, component = await syncai_api.search_constitutional_committee(
        query=query,
        limit=limit,
        page=page
    )
    if err:
        return f"Could not search CC members, err: {err}"
    
    main_key = "data"
    result_text = f"Constitutional Committee Search Results\n"
    result_text += format_results(data.get(main_key, []))
    return result_text

@mcp.tool(
    name="search_proposals",
    description="""Search for Cardano blockchain governance proposals using natural language queries.

    Use this tool when users ask about:
    - Finding proposals by type (treasury, network parameters, info actions, etc.)
    - Proposals with specific voting patterns
    - Recent or historical governance actions
    - Proposals by voting outcome (passed, failed, pending)

    Example Queries:
    - "Show me the latest governance proposals"
    - "Find all treasury withdrawal proposals"
    - "What proposals are currently active?"
    - "Show proposals about network parameter changes"
    - "Which proposals passed with over 80% yes votes?"

    Returns: Paginated list of proposals with voting data, status, and details.
    """
)
async def search_proposals(
    query: str = Field(
        ...,
        description="Natural language query about blockchain governance proposals",
        examples=[
            "give me proposals named net change",
            "show me all parameter change proposals",
            "proposals where more than 80% SPOs voted no",
            "treasury withdrawal proposals from 2025"
        ]
    ),
    limit: int = Field(
        default=10,
        description="Number of results to return",
        ge=1,
        le=100
    ),
    page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    )
):
    data, err, component = await syncai_api.search_proposals(
        query=query,
        limit=limit,
        page=page
    )
    if err:
        return f"Could not search proposals, err: {err}"
    
    main_key = "data"
    result_text = f"Proposal Search Results\n"
    result_text += format_results(data.get(main_key, []))
    return result_text

@mcp.tool(name="analyze_proposal", description="Generate AI analysis of blockchain governance proposals and topics.")
async def analyze_proposal(
    query: str = Field(
        ...,
        description="Natural language question or topic for AI analysis",
        examples=[
            "ARTICLE IV. THE CARDANO BLOCKCHAIN ECOSYSTEM",
            "Analyze the voting patterns for this proposal"
        ]
    ),
    govaction_id: Optional[str] = Field(
        default=None,
        description="Optional specific governance action ID",
        examples=["gov_action1js2s9v92zpxg2rge0y3jt9zy626he2m67x9kx9phw4r942kvsn6sqfym0d7"]
    )
):
    data, err, component = await syncai_api.analyze_proposal(
        query=query,
        govaction_id=govaction_id
    )
    if err:
        return f"Could not analyze proposal, err: {err}"
    
    main_key = "data"
    result_text = f"Proposal Analysis\n"
    result_text += format_results(data.get(main_key, {}))
    return result_text

@mcp.tool(name="analyze_voter_rationale", description="Analyze voter rationales for governance proposals.")
async def analyze_voter_rationale(
    govaction_id: str = Field(
        ...,
        description="Governance action ID to analyze rationales for",
        examples=["gov_action10ueqgzwenxr39le68n0se9peu92r7gm2846xwehh3u0ahc0qd0uqqyljxu5"]
    ),
    vote_type: str = Field(
        ...,
        description="Type of vote to analyze ('yes' or 'no')",
        examples=["yes", "no"]
    ),
    for_role: str = Field(
        default="all",
        description="Analyze rationales for specific role",
        examples=["all", "drep", "spo", "cc"]
    ),
    custom_system_prompt: Optional[str] = Field(
        default=None,
        description="Optional custom system prompt for analysis"
    )
):
    data, err, component = await syncai_api.analyze_voter_rationale(
        govaction_id=govaction_id,
        vote_type=vote_type,
        for_role=for_role,
        custom_system_prompt=custom_system_prompt
    )
    if err:
        return f"Could not analyze rationales, err: {err}"
    
    main_key = "data"
    result_text = f"Rationale Analysis\n"
    result_text += format_results(data.get(main_key, {}))
    return result_text

@mcp.tool(name="search_governance_knowledge", description="Search Cardano governance documents and get AI analysis.")
async def search_governance_knowledge(
    query: str = Field(
        ...,
        description="Search question about Cardano governance",
        examples=[
            "What is governance voting in Cardano?",
            "How do DReps support Cardano governance?",
            "What are protocol parameter changes?"
        ]
    )
):
    data, err, component = await syncai_api.search_governance_knowledge(query=query)
    if err:
        return f"Could not search governance knowledge, err: {err}"
    
    main_key = "data"
    result_text = f"Governance Knowledge Search\n"
    result_text += format_results(data.get(main_key, {}))
    return result_text

@mcp.tool(name="get_trend_data", description="Get growth trend data for DReps and stake pools over time.")
@format_results_decorator
async def get_trend_data(
    trend_type: str = Field(
        default="all",
        description="Type of trend data to retrieve",
        examples=["all", "drep", "spo", "pool"]
    )
):
    return await syncai_api.get_trend_data(trend_type=trend_type)

@mcp.tool(name="get_drep_country_distribution", description="Get geographic distribution of DReps by country.")
@format_results_decorator
async def get_drep_country_distribution():
    return await syncai_api.get_drep_country_distribution()

@mcp.tool(name="get_proposal_voting_history", description="Get detailed voting history for a specific proposal by voter role.")
async def get_proposal_voting_history(
    gov_id: str = Field(
        ...,
        description="Proposal ID to get voting history for",
        examples=["gov_action1nd3t833j7v5sz65k3tp9yyvztw60sjcjgcgjr37682s3m7frwrusqmd2k80"]
    ),
    role: str = Field(
        ...,
        description="Voter role to filter by ('spo', 'drep', or 'cc')",
        examples=["spo", "drep", "cc"]
    ),
    page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    limit: int = Field(
        default=10,
        description="Results per page by defalut use 10",
        ge=1,
        le=100
    )
):
    data, err, component = await syncai_api.get_proposal_voting_history(
        gov_id=gov_id,
        role=role,
        page=page,
        limit=limit
    )
    if err:
        return f"Could not get voting history, err: {err}"
    
    main_key = "votingHistory"
    result_text = f"Proposal Voting History\n"
    result_text += format_results(data.get(main_key, []))
    return result_text

@mcp.tool(name="get_user_voting_history", description="Get voting history for specific users across all proposals.")
async def get_user_voting_history(
    user_id: Union[str, List[str]] = Field(
        ...,
        description="User ID(s) - SPO pool_id, DRep drep_id, or CC cc_id",
        examples=[
            "pool1wyfau3rlkckr3clr4mkyxn9vdlrkup0fj7uyyflfccmms24gm3a",
            ["pool1abc...", "drep1xyz..."]
        ]
    ),
    page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    limit: int = Field(
        default=10,
        description="Results per page",
        ge=1,
        le=100
    )
):
    data, err, component = await syncai_api.get_user_voting_history(
        user_id=user_id,
        page=page,
        limit=limit
    )
    if err:
        return f"Could not get user voting history, err: {err}"
    
    main_key = "votingHistory"
    result_text = f"User Voting History\n"
    result_text += format_results(data.get(main_key, []))
    return result_text

@mcp.tool(name="get_proposal_rationales", description="Get all user rationales for a specific proposal, filterable by role.")
async def get_proposal_rationales(
    gov_id: str = Field(
        ...,
        description="Proposal ID to get rationales for",
        examples=["gov_action1nd3t833j7v5sz65k3tp9yyvztw60sjcjgcgjr37682s3m7frwrusqmd2k80"]
    ),
    role: str = Field(
        default="all",
        description="Filter rationales by role",
        examples=["all", "spo", "drep", "cc"]
    ),
    page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    limit: int = Field(
        default=10,
        description="Results per page",
        ge=1,
        le=100
    )
):
    data, err, component = await syncai_api.get_proposal_rationales(
        gov_id=gov_id,
        role=role,
        page=page,
        limit=limit
    )
    if err:
        return f"Could not get proposal rationales, err: {err}"
    
    main_key = "rationales"
    result_text = f"Proposal Rationales\n"
    result_text += format_results(data.get(main_key, []))
    return result_text

@mcp.tool(name="get_user_rationales", description="Get all rationales from a specific user across all proposals.")
async def get_user_rationales(
    user_id: str = Field(
        ...,
        description="User ID - pool_id, drep_id, or cc_id",
        examples=[
            "drep1y2200we9c904un36tzaearntzzl63snffuul9qsk0te4utqfkke0w",
            "pool1wyfau3rlkckr3clr4mkyxn9vdlrkup0fj7uyyflfccmms24gm3a"
        ]
    ),
    page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    limit: int = Field(
        default=10,
        description="Results per page",
        ge=1,
        le=100
    )
):
    data, err, component = await syncai_api.get_user_rationales(
        user_id=user_id,
        page=page,
        limit=limit
    )
    if err:
        return f"Could not get user rationales, err: {err}"
    
    main_key = "rationales"
    result_text = f"User Rationales\n"
    result_text += format_results(data.get(main_key, []))
    return result_text

@mcp.tool(name="get_ada_power_history", description="Get historical ADA power data for pools and DReps over time.")
async def get_ada_power_history(
    entity_id: Union[str, List[str]] = Field(
        ...,
        description="Single ID, array of IDs, or comma-separated string of pool/drep IDs",
        examples=[
            "pool1q8fqknvzyj9p4ezhxqdghlc7rnmwvwuku9eevgvl25cpjy385gg",
            ["drep1y2200we9c904un36tzaearntzzl63snffuul9qsk0te4utqfkke0w", "pool1q8fqknvzyj9p4ezhxqdghlc7rnmwvwuku9eevgvl25cpjy385gg"],
            "drep1yt8mj24jjmlz33rllr28m54fj4msyr27srzjst87egtwtrs8almw3,pool1c3fjkls7d2aujud8y5xy5e0azu0ueatwn34u7jy3ql85ze3xya8"
        ]
    )
):
    data, err, component = await syncai_api.get_ada_power_history(entity_id=entity_id)
    if err:
        return f"Could not get ADA power history, err: {err}"
    
    # Handle both single and multiple entity responses
    if isinstance(data, dict) and "data" in data:
        main_key = "data"
        result_text = f"ADA Power History (Multiple)\n"
        result_text += format_results(data.get(main_key, []))
    else:
        result_text = f"ADA Power History (Single)\n"
        result_text += format_results(data)
    
    return result_text

#________________________________________MCP TOOLS__Start________________________
@mcp.tool(
    name="account_overview",
    description="""Get a comprehensive summary of a Cardano stake account (stake address).

    Use this tool when users ask about:
    - Stake account balance and total rewards
    - Delegation status and pool information
    - Account activity and history
    - Staking rewards earned
    - Current stake pool delegation

    IMPORTANT: This is for stake addresses (stake1...), not payment addresses (addr1...).
    For payment addresses, use address_explorer instead.

    Example Queries:
    - "What is the balance of stake address stake1u9..."
    - "Show me rewards for stake account stake1..."
    - "Is this stake account delegated?"
    - "Get overview of stake1u9..."

    Example Input:
    stake_address: "stake1u9r76ypf5fskppa0cmttas05cgcswrpwqgrgzxkhp4njgs9rx4jvf"

    Returns: Comprehensive account summary with balance, rewards, delegation, and optionally assets.
    """
)
async def account_overview(
    stake_address: str,
    include_rewards: bool = True,
    include_delegations: bool = True,
    include_assets: bool = False,
    summary_only: bool = False,
    chunk_output: bool = None,
    skip_problematic: bool = True,  # New parameter
    max_processing_time: int = 60,  # New parameter
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),                 # New parameter
    page_size: int = None          # New parameter
) -> List[types.TextContent]:
    """
    Get a comprehensive summary of a Cardano stake account including balance, rewards, and delegation status.
    
    Args:
        stake_address: Cardano stake address
        include_rewards: Whether to include reward history
        include_delegations: Whether to include delegation history
        include_assets: Whether to include asset information
        summary_only: Return only summary information
        chunk_output: Whether to split large output into chunks
        skip_problematic: Skip addresses/assets that cause errors
        max_processing_time: Maximum time (seconds) to spend processing
        page: Page number for pagination
        page_size: Items per page for paginated results
    """
    results = []
    processing_start_time = time.time()
    
    try:
        logger.info(f"Getting account overview for {stake_address}")
        
        # 1. Get the basic account data (using the fetch_and_cache_data helper)
        account_data = None
        account_items = await fetch_and_cache_data(
            endpoint=f"accounts/{stake_address}",
            cache_table='cache_accounts',
            key_field='stake_address',
            key_value=stake_address,
            ttl_hours=12,  # Cache for 12 hours
            use_expired_cache=True  # Use expired cache data as fallback
        )
        
        if account_items and len(account_items) > 0:
            account_data = account_items[0]
        else:
            return [types.TextContent(
                type="text",
                text=f"Error retrieving account information for {stake_address}"
            )]
        
        # Build the overview text
        overview = f"# Account Overview: {stake_address}\n\n"
        
        overview += "## Basic Information\n"
        overview += f"- Status: {'Active' if account_data.get('active', False) else 'Inactive'}\n"
        
        # Safely convert lovelace to ADA
        controlled_amount = lovelace_to_ada(account_data.get('controlled_amount', 0))
        rewards_sum = lovelace_to_ada(account_data.get('rewards_sum', 0))
        withdrawals_sum = lovelace_to_ada(account_data.get('withdrawals_sum', 0))
        withdrawable_amount = lovelace_to_ada(account_data.get('withdrawable_amount', 0))
        
        overview += f"- Controlled Amount: {account_data.get('controlled_amount', 0)} lovelace ({controlled_amount:.6f} ADA)\n"
        overview += f"- Total Rewards: {account_data.get('rewards_sum', 0)} lovelace ({rewards_sum:.6f} ADA)\n"
        overview += f"- Total Withdrawals: {account_data.get('withdrawals_sum', 0)} lovelace ({withdrawals_sum:.6f} ADA)\n"
        overview += f"- Withdrawable Amount: {account_data.get('withdrawable_amount', 0)} lovelace ({withdrawable_amount:.6f} ADA)\n"
        
        # 2. Get pool information if delegated
        if account_data.get('pool_id'):
            overview += f"- Delegated to Pool: {account_data.get('pool_id')}\n"
            
            try:
                # Check timing
                current_time = time.time()
                if current_time - processing_start_time > max_processing_time:
                    logger.warning(f"Maximum processing time ({max_processing_time}s) exceeded for pool info.")
                    overview += "  - Pool information retrieval skipped due to time constraints\n"
                else:
                    # Get pool information - cache this separately
                    pool_items = await fetch_and_cache_data(
                        endpoint=f"pools/{account_data['pool_id']}",
                        cache_table='cache_pools',
                        key_field='pool_id',
                        key_value=account_data['pool_id'],
                        ttl_hours=24,  # Cache pool data for 24 hours
                        use_expired_cache=True  # Use expired cache data as fallback
                    )
                    
                    if pool_items and len(pool_items) > 0:
                        # Try to get pool metadata
                        pool_metadata_items = await fetch_and_cache_data(
                            endpoint=f"pools/{account_data['pool_id']}/metadata",
                            cache_table='cache_pools_extended',
                            key_field='pool_id',
                            key_value=account_data['pool_id'],
                            ttl_hours=24,  # Cache for 24 hours
                            use_expired_cache=True  # Use expired cache data as fallback
                        )
                        
                        if pool_metadata_items and len(pool_metadata_items) > 0:
                            pool_metadata = pool_metadata_items[0]
                            overview += f"  - Pool Name: {pool_metadata.get('name', 'Unknown')}\n"
                            overview += f"  - Pool Description: {pool_metadata.get('description', 'No description')}\n"
            except Exception as e:
                if skip_problematic:
                    logger.error(f"Error getting pool info: {e}")
                    overview += "  - Pool information unavailable\n"
                else:
                    raise
        else:
            overview += "- Not currently delegated to any pool\n"
        
        # If summary_only is True, return just the basic information
        if summary_only:
            results.append(types.TextContent(type="text", text=overview))
            return results
        
        # 3. Get reward history if requested
        if include_rewards:
            try:
                # Check timing
                current_time = time.time()
                if current_time - processing_start_time > max_processing_time:
                    logger.warning(f"Maximum processing time ({max_processing_time}s) exceeded for rewards.")
                    overview += "\n## Rewards\nRewards retrieval skipped due to time constraints.\n"
                else:
                    rewards_data = await fetch_and_cache_data(
                        endpoint=f"accounts/{stake_address}/rewards",
                        cache_table='cache_account_rewards',
                        key_field='stake_address',
                        key_value=stake_address,
                        validator=validate_reward_data,
                        order_field='epoch',
                        params={"count": 20},
                        ttl_hours=6,  # Cache for 6 hours since rewards update regularly
                        use_expired_cache=True  # Use expired cache data as fallback
                    )
                    
                    if rewards_data and len(rewards_data) > 0:
                        # Apply pagination to rewards if needed
                        effective_page_size = get_config_value("page_size", page_size)
                        paginator = PaginatedResponse(rewards_data, effective_page_size)
                        page_data = paginator.get_page(page)
                        
                        overview += "\n## Recent Rewards\n"
                        if page_data['total_pages'] > 1:
                            overview += f"Showing page {page_data['page']} of {page_data['total_pages']} "
                            overview += f"(total: {page_data['total_items']} rewards)\n\n"
                            
                        overview += "| Epoch | Amount (ADA) | Pool ID |\n"
                        overview += "|-------|-------------|--------|\n"
                        
                        for reward in page_data['items']:
                            epoch = reward.get('epoch', 'Unknown')
                            amount = reward.get('amount', 0)
                            reward_ada = lovelace_to_ada(amount)
                            pool_id = reward.get('pool_id', 'Unknown')
                            overview += f"| {epoch} | {reward_ada:.6f} | {pool_id} |\n"
                            
                        # Add pagination info if applicable
                        if page_data['total_pages'] > 1:
                            overview += format_pagination_info(page_data)
                    else:
                        overview += "\n## Rewards\nNo rewards found for this account.\n"
            except Exception as e:
                if skip_problematic:
                    logger.error(f"Error getting rewards: {e}")
                    overview += "\n## Rewards\nError retrieving rewards information.\n"
                else:
                    raise
        
        # 4. Get delegation history if requested
        if include_delegations:
            try:
                # Check timing
                current_time = time.time()
                if current_time - processing_start_time > max_processing_time:
                    logger.warning(f"Maximum processing time ({max_processing_time}s) exceeded for delegations.")
                    overview += "\n## Delegations\nDelegations retrieval skipped due to time constraints.\n"
                else:
                    delegations_data = await fetch_and_cache_data(
                        endpoint=f"accounts/{stake_address}/delegations",
                        cache_table='cache_account_delegations',
                        key_field='stake_address',
                        key_value=stake_address,
                        validator=validate_delegation_data,
                        order_field='active_epoch',
                        params={"count": 10},
                        ttl_hours=12,  # Cache for 12 hours
                        use_expired_cache=True  # Use expired cache data as fallback
                    )
                    
                    if delegations_data and len(delegations_data) > 0:
                        overview += "\n## Recent Delegations\n"
                        overview += "| Active Epoch | Pool ID | Transaction |\n"
                        overview += "|--------------|---------|-------------|\n"
                        
                        for delegation in delegations_data:
                            active_epoch = delegation.get('active_epoch', 'Unknown')
                            tx_hash = delegation.get('tx_hash', 'Unknown')
                            pool_id = delegation.get('pool_id', 'Unknown')
                            overview += f"| {active_epoch} | {pool_id} | {tx_hash[:8]}... |\n"
                    else:
                        overview += "\n## Delegations\nNo delegation history found for this account.\n"
            except Exception as e:
                if skip_problematic:
                    logger.error(f"Error getting delegations: {e}")
                    overview += "\n## Delegations\nError retrieving delegation information.\n"
                else:
                    raise
        
        # 5. Get asset information if requested
        if include_assets:
            try:
                # Check timing
                current_time = time.time()
                if current_time - processing_start_time > max_processing_time:
                    logger.warning(f"Maximum processing time ({max_processing_time}s) exceeded for assets.")
                    overview += "\n## Assets\nAssets retrieval skipped due to time constraints.\n"
                else:
                    # First get all addresses associated with this stake address
                    addresses_data = await fetch_and_cache_data(
                        endpoint=f"accounts/{stake_address}/addresses",
                        cache_table='cache_account_addresses',
                        key_field='stake_address',
                        key_value=stake_address,
                        validator=validate_address_data,
                        params={"count": 50},
                        ttl_hours=12,  # Cache for 12 hours
                        use_expired_cache=True  # Use expired cache data as fallback
                    )
                    
                    if addresses_data and len(addresses_data) > 0:
                        overview += "\n## Assets Summary\n"
                        
                        # Track unique assets across all addresses
                        assets_summary = {}
                        problematic_addresses = 0
                        
                        for address_info in addresses_data:
                            address = address_info.get('address')
                            if not address:
                                continue
                                
                            # Check if we've run out of processing time
                            current_time = time.time()
                            if current_time - processing_start_time > max_processing_time:
                                overview += f"Processing time limit reached. Only {len(assets_summary)} assets collected.\n"
                                break
                                
                            try:
                                # Get assets at this address - cache each address's assets separately
                                address_assets_items = await fetch_and_cache_data(
                                    endpoint=f"addresses/{address}/extended",
                                    cache_table='cache_address_assets',
                                    key_field='address',
                                    key_value=address,
                                    ttl_hours=6,  # Cache for 6 hours since assets can change
                                    timeout=10.0,  # Shorter timeout
                                    use_expired_cache=True  # Use expired cache data as fallback
                                )
                                
                                if address_assets_items and len(address_assets_items) > 0:
                                    address_assets = address_assets_items[0]
                                    
                                    # Extract non-lovelace assets (tokens)
                                    if 'amount' in address_assets and isinstance(address_assets['amount'], list):
                                        for amount_entry in address_assets['amount']:
                                            if 'unit' in amount_entry and amount_entry['unit'] != 'lovelace':
                                                asset_unit = amount_entry.get('unit', 'unknown')
                                                asset_quantity = int(amount_entry.get('quantity', 0))
                                                
                                                if asset_unit in assets_summary:
                                                    assets_summary[asset_unit]['quantity'] += asset_quantity
                                                else:
                                                    # Initialize new asset entry
                                                    assets_summary[asset_unit] = {
                                                        'quantity': asset_quantity,
                                                        'policy_id': asset_unit[:56] if len(asset_unit) > 56 else asset_unit,
                                                        'asset_name': asset_unit[56:] if len(asset_unit) > 56 else ''
                                                    }
                            except Exception as addr_error:
                                logger.error(f"Error processing address {address}: {addr_error}")
                                if skip_problematic:
                                    problematic_addresses += 1
                                    continue
                                else:
                                    raise
                        
                        # Display assets summary
                        if assets_summary:
                            # If too many assets, provide a summary instead of full list
                            if len(assets_summary) > 50:
                                overview += f"\nAccount has {len(assets_summary)} unique assets.\n"
                                
                                # Group by policy
                                policies = {}
                                for asset_unit, details in assets_summary.items():
                                    policy_id = details['policy_id']
                                    if policy_id in policies:
                                        policies[policy_id]['count'] += 1
                                        policies[policy_id]['tokens'].append(asset_unit)
                                    else:
                                        policies[policy_id] = {'count': 1, 'tokens': [asset_unit]}
                                
                                overview += f"Assets grouped by {len(policies)} policies:\n\n"
                                
                                overview += "| Policy ID | Asset Count |\n"
                                overview += "|-----------|-------------|\n"
                                
                                # Show top 10 policies by count
                                top_policies = sorted(policies.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
                                for policy_id, data in top_policies:
                                    overview += f"| {policy_id[:8]}... | {data['count']} |\n"
                                
                                if len(policies) > 10:
                                    overview += f"\n*Showing top 10 of {len(policies)} policies*\n"
                            else:
                                overview += "\n### Native Tokens\n"
                                overview += "| Token | Policy ID | Quantity |\n"
                                overview += "|-------|-----------|----------|\n"
                                
                                for asset_unit, details in assets_summary.items():
                                    # Try to get a better name for the token
                                    display_name = asset_unit
                                    
                                    try:
                                        # Get token metadata if within time limit
                                        current_time = time.time()
                                        if current_time - processing_start_time <= max_processing_time:
                                            asset_details_items = await fetch_and_cache_data(
                                                endpoint=f"assets/{asset_unit}",
                                                cache_table='cache_assets',
                                                key_field='asset',
                                                key_value=asset_unit,
                                                ttl_hours=24,  # Cache for 24 hours
                                                timeout=5.0,   # Short timeout
                                                use_expired_cache=True  # Use expired cache data as fallback
                                            )
                                            
                                            if asset_details_items and len(asset_details_items) > 0:
                                                asset_details = asset_details_items[0]
                                                if 'onchain_metadata' in asset_details and asset_details['onchain_metadata']:
                                                    if 'name' in asset_details['onchain_metadata']:
                                                        display_name = asset_details['onchain_metadata']['name']
                                                elif 'metadata' in asset_details and asset_details['metadata']:
                                                    if 'name' in asset_details['metadata']:
                                                        display_name = asset_details['metadata']['name']
                                    except Exception as e:
                                        if not skip_problematic:
                                            raise
                                    
                                    overview += f"| {display_name} | {details['policy_id'][:8]}... | {details['quantity']} |\n"
                            
                            if problematic_addresses > 0:
                                overview += f"\n⚠️ Note: {problematic_addresses} addresses were skipped due to errors.\n"
                        else:
                            overview += "\nNo native tokens found in this account.\n"
                    else:
                        overview += "\n## Assets\nNo address information found for this account.\n"
            except Exception as e:
                if skip_problematic:
                    logger.error(f"Error getting assets: {e}")
                    overview += "\n## Assets\nError retrieving asset information.\n"
                else:
                    raise
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        if should_chunk:
            chunks = chunk_text(overview)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=overview))
            
        return results
    
    except Exception as e:
        logger.error(f"Error in account_overview: {e}")
        logger.error(traceback.format_exc())
        
        error_text = f"# Error Retrieving Account Overview\n\n"
        error_text += f"An error occurred while retrieving the account overview: {str(e)}\n\n"
        error_text += "To work around this issue, you can try:\n"
        error_text += "1. Using summary_only=True to get just basic information\n"
        error_text += "2. Setting skip_problematic=True to skip problem areas\n"
        error_text += "3. Disabling specific sections (include_rewards=False, include_assets=False)\n"
        error_text += "4. Increasing max_processing_time if needed\n\n"
        
        return [types.TextContent(type="text", text=error_text)]



@mcp.tool(name="account_rewards_analysis", description="Analyze rewards distribution and performance for a Cardano stake address.")
async def account_rewards_analysis(stake_address: str, epochs: int = 30) -> List[types.TextContent]:
    """
    Analyze rewards distribution and performance for a Cardano stake address.
    """
    try:
        # Get rewards history using the cached data helper
        rewards_data = await fetch_and_cache_data(
            endpoint=f"accounts/{stake_address}/rewards",
            cache_table='cache_account_rewards',
            key_field='stake_address',
            key_value=stake_address,
            validator=validate_reward_data,
            order_field='epoch',
            params={"count": epochs},
            ttl_hours=6  # Cache for 6 hours
        )
        
        if not rewards_data or len(rewards_data) == 0:
            return [types.TextContent(
                type="text",
                text=f"No rewards data found for stake address {stake_address} in the last {epochs} epochs."
            )]
        
        # Get account information for delegation details
        account_data = None
        account_items = await fetch_and_cache_data(
            endpoint=f"accounts/{stake_address}",
            cache_table='cache_accounts',
            key_field='stake_address',
            key_value=stake_address,
            ttl_hours=12  # Cache for 12 hours
        )
        
        if account_items and len(account_items) > 0:
            account_data = account_items[0]
        
        # Calculate metrics
        total_rewards = sum(int(reward.get('amount', 0)) for reward in rewards_data)
        avg_reward = total_rewards / len(rewards_data) if rewards_data else 0
        
        # Group rewards by pool
        pools_rewards = {}
        for reward in rewards_data:
            pool_id = reward.get('pool_id', 'Unknown')
            amount = int(reward.get('amount', 0))
            if pool_id in pools_rewards:
                pools_rewards[pool_id]['amount'] += amount
                pools_rewards[pool_id]['count'] += 1
            else:
                pools_rewards[pool_id] = {'amount': amount, 'count': 1}
        
        # Find best and worst performing pools
        best_pool = max(pools_rewards.items(), key=lambda x: x[1]['amount']) if pools_rewards else (None, None)
        worst_pool = min(pools_rewards.items(), key=lambda x: x[1]['amount']) if pools_rewards else (None, None)
        
        # Find highest and lowest single rewards
        highest_reward = max(rewards_data, key=lambda x: int(x.get('amount', 0))) if rewards_data else None
        lowest_reward = min(rewards_data, key=lambda x: int(x.get('amount', 0))) if rewards_data else None
        
        # Generate analysis text
        analysis_text = f"# Rewards Analysis for {stake_address}\n\n"
        
        analysis_text += "## Summary Statistics\n"
        analysis_text += f"- Total Rewards (Last {epochs} epochs): {lovelace_to_ada(total_rewards):.6f} ADA\n"
        analysis_text += f"- Average Reward per Epoch: {lovelace_to_ada(avg_reward):.6f} ADA\n"
        analysis_text += f"- Number of Epochs with Rewards: {len(rewards_data)}\n"
        
        if account_data:
            analysis_text += f"- Currently Delegated to: {account_data.get('pool_id', 'Not delegated')}\n\n"
        
        analysis_text += "## Rewards by Pool\n"
        analysis_text += "| Pool ID | Total Rewards (ADA) | Epochs | Avg. per Epoch |\n"
        analysis_text += "|---------|---------------------|--------|---------------|\n"
        
        for pool_id, data in pools_rewards.items():
            total = lovelace_to_ada(data['amount'])
            count = data['count']
            avg = total / count if count > 0 else 0
            analysis_text += f"| {pool_id} | {total:.6f} | {count} | {avg:.6f} |\n"
        
        if best_pool[0]:
            analysis_text += f"\n- Best Performing Pool: {best_pool[0]} with {lovelace_to_ada(best_pool[1]['amount']):.6f} ADA total\n"
        if worst_pool[0]:
            analysis_text += f"- Lowest Performing Pool: {worst_pool[0]} with {lovelace_to_ada(worst_pool[1]['amount']):.6f} ADA total\n"
        
        if highest_reward:
            analysis_text += f"\n## Highest Single Epoch Reward\n"
            analysis_text += f"- Epoch: {highest_reward.get('epoch', 'Unknown')}\n"
            analysis_text += f"- Amount: {lovelace_to_ada(highest_reward.get('amount', 0)):.6f} ADA\n"
            analysis_text += f"- Pool: {highest_reward.get('pool_id', 'Unknown')}\n"
        
        if lowest_reward:
            analysis_text += f"\n## Lowest Single Epoch Reward\n"
            analysis_text += f"- Epoch: {lowest_reward.get('epoch', 'Unknown')}\n"
            analysis_text += f"- Amount: {lovelace_to_ada(lowest_reward.get('amount', 0)):.6f} ADA\n"
            analysis_text += f"- Pool: {lowest_reward.get('pool_id', 'Unknown')}\n"
        
        analysis_text += "\n## Recent Rewards Trend (Newest to Oldest)\n"
        analysis_text += "| Epoch | Amount (ADA) | Pool ID |\n"
        analysis_text += "|-------|-------------|--------|\n"
        
        # Sort rewards by epoch (descending)
        sorted_rewards = sorted(rewards_data, key=lambda x: x.get('epoch', 0), reverse=True)
        for reward in sorted_rewards[:10]:  # Show last 10 epochs
            epoch = reward.get('epoch', 'Unknown')
            amount = lovelace_to_ada(reward.get('amount', 0))
            pool_id = reward.get('pool_id', 'Unknown')
            analysis_text += f"| {epoch} | {amount:.6f} | {pool_id} |\n"
        
        return [types.TextContent(type="text", text=analysis_text)]
        
    except Exception as e:
        logger.error(f"Error in account_rewards_analysis: {e}")
        logger.error(traceback.format_exc())
        return [types.TextContent(
            type="text",
            text=f"An error occurred while analyzing account rewards: {str(e)}"
        )]


@mcp.tool(name="account_delegation_tracker", description="Track delegation changes and rewards for a Cardano stake address.")
async def account_delegation_tracker(stake_address: str, delegations_limit: int = 10) -> List[types.TextContent]:
    """
    Track delegation changes and rewards for a Cardano stake address.
    """
    try:
        # Get delegation history with caching
        delegations_data = await fetch_and_cache_data(
            endpoint=f"accounts/{stake_address}/delegations",
            cache_table='cache_account_delegations',
            key_field='stake_address',
            key_value=stake_address,
            validator=validate_delegation_data,
            order_field='active_epoch',
            params={"count": delegations_limit},
            ttl_hours=12  # Cache for 12 hours
        )
        
        if not delegations_data or len(delegations_data) == 0:
            return [types.TextContent(
                type="text",
                text=f"No delegation history found for stake address {stake_address}."
            )]
        
        # Get rewards history for correlation
        rewards_data = await fetch_and_cache_data(
            endpoint=f"accounts/{stake_address}/rewards",
            cache_table='cache_account_rewards',
            key_field='stake_address',
            key_value=stake_address,
            validator=validate_reward_data,
            order_field='epoch',
            params={"count": 100},
            ttl_hours=6  # Cache for 6 hours
        )
        
        # Generate tracking text
        tracking_text = f"# Delegation Tracking for {stake_address}\n\n"
        
        tracking_text += "## Delegation History\n"
        tracking_text += "| Active Epoch | Pool ID | Transaction |\n"
        tracking_text += "|--------------|---------|-------------|\n"
        
        # Sort delegations by active epoch (descending)
        sorted_delegations = sorted(delegations_data, key=lambda x: x.get('active_epoch', 0), reverse=True)
        
        for delegation in sorted_delegations:
            active_epoch = delegation.get('active_epoch', 'Unknown')
            tx_hash = delegation.get('tx_hash', 'Unknown')
            pool_id = delegation.get('pool_id', 'Unknown')
            tracking_text += f"| {active_epoch} | {pool_id} | {tx_hash[:8]}... |\n"
        
        # Correlation between delegations and rewards
        if rewards_data and len(rewards_data) > 0:
            tracking_text += "\n## Rewards by Delegation\n"
            
            # Group delegations by pool to determine epoch ranges
            pool_epochs = {}
            for i, delegation in enumerate(sorted_delegations):
                active_epoch = delegation.get('active_epoch', 0)
                pool_id = delegation.get('pool_id', 'Unknown')
                
                # Determine end epoch (either next delegation's active epoch - 1, or current epoch if latest)
                end_epoch = float('inf')  # Default to infinity for most recent delegation
                if i > 0:
                    end_epoch = sorted_delegations[i-1].get('active_epoch', 0) - 1
                
                pool_epochs[pool_id] = pool_epochs.get(pool_id, []) + [(active_epoch, end_epoch)]
            
            # For each pool, calculate rewards during its delegation period
            pools_summary = {}
            for pool_id, epochs_ranges in pool_epochs.items():
                total_rewards = 0
                epochs_count = 0
                
                for start_epoch, end_epoch in epochs_ranges:
                    # Count rewards for this pool during this epoch range
                    for reward in rewards_data:
                        reward_epoch = reward.get('epoch', 0)
                        reward_pool = reward.get('pool_id', 'Unknown')
                        reward_amount = int(reward.get('amount', 0))
                        
                        if reward_pool == pool_id and start_epoch <= reward_epoch <= end_epoch:
                            total_rewards += reward_amount
                            epochs_count += 1
                
                pools_summary[pool_id] = {
                    'total_rewards': total_rewards,
                    'epochs_count': epochs_count,
                    'avg_reward': total_rewards / epochs_count if epochs_count > 0 else 0
                }
            
            # Display pool performance
            tracking_text += "| Pool ID | Total Rewards (ADA) | Reward Epochs | Avg. per Epoch |\n"
            tracking_text += "|---------|---------------------|--------------|---------------|\n"
            
            for pool_id, summary in pools_summary.items():
                total = lovelace_to_ada(summary['total_rewards'])
                count = summary['epochs_count']
                avg = lovelace_to_ada(summary['avg_reward'])
                tracking_text += f"| {pool_id} | {total:.6f} | {count} | {avg:.6f} |\n"
            
            # Find best performing pool
            best_pool = max(pools_summary.items(), key=lambda x: x[1]['avg_reward']) if pools_summary else (None, None)
            if best_pool[0]:
                tracking_text += f"\n- Best Performing Pool: {best_pool[0]} with average {lovelace_to_ada(best_pool[1]['avg_reward']):.6f} ADA per epoch\n"
        
        return [types.TextContent(type="text", text=tracking_text)]
        
    except Exception as e:
        logger.error(f"Error in account_delegation_tracker: {e}")
        logger.error(traceback.format_exc())
        return [types.TextContent(
            type="text",
            text=f"An error occurred while tracking delegations: {str(e)}"
        )]


@mcp.tool(name="account_history", description="View historical activities of a Cardano stake address including rewards, delegations, and registrations.")
async def account_history(stake_address: str, history_type: str = "all", limit: int = 50) -> List[types.TextContent]:
    """
    View historical activities of a Cardano stake address.
    """
    try:
        history_text = f"# Account History for {stake_address}\n\n"
        
        # Get rewards history if requested
        if history_type in ["all", "rewards"]:
            rewards_data = await fetch_and_cache_data(
                endpoint=f"accounts/{stake_address}/rewards",
                cache_table='cache_account_rewards',
                key_field='stake_address',
                key_value=stake_address,
                validator=validate_reward_data,
                order_field='epoch',
                params={"count": limit},
                ttl_hours=6  # Cache for 6 hours
            )
            
            if rewards_data and len(rewards_data) > 0:
                history_text += "## Reward History\n"
                history_text += "| Epoch | Amount (ADA) | Pool ID |\n"
                history_text += "|-------|-------------|--------|\n"
                
                for reward in rewards_data:
                    epoch = reward.get('epoch', 'Unknown')
                    amount = lovelace_to_ada(reward.get('amount', 0))
                    pool_id = reward.get('pool_id', 'Unknown')
                    history_text += f"| {epoch} | {amount:.6f} | {pool_id} |\n"
            elif history_type == "rewards":
                history_text += "## Reward History\nNo rewards found for this account.\n\n"
        
        # Get delegation history if requested
        if history_type in ["all", "delegations"]:
            delegations_data = await fetch_and_cache_data(
                endpoint=f"accounts/{stake_address}/delegations",
                cache_table='cache_account_delegations',
                key_field='stake_address',
                key_value=stake_address,
                validator=validate_delegation_data,
                order_field='active_epoch',
                params={"count": limit},
                ttl_hours=12  # Cache for 12 hours
            )
            
            if delegations_data and len(delegations_data) > 0:
                history_text += "\n## Delegation History\n"
                history_text += "| Active Epoch | Pool ID | Transaction |\n"
                history_text += "|--------------|---------|-------------|\n"
                
                for delegation in delegations_data:
                    active_epoch = delegation.get('active_epoch', 'Unknown')
                    tx_hash = delegation.get('tx_hash', 'Unknown')
                    pool_id = delegation.get('pool_id', 'Unknown')
                    history_text += f"| {active_epoch} | {pool_id} | {tx_hash[:8]}... |\n"
            elif history_type == "delegations":
                history_text += "## Delegation History\nNo delegations found for this account.\n\n"
        
        # Get registration history if requested
        if history_type in ["all", "registrations"]:
            registrations_data = await fetch_and_cache_data(
                endpoint=f"accounts/{stake_address}/registrations",
                cache_table='cache_account_registrations',
                key_field='stake_address',
                key_value=stake_address,
                validator=validate_registration_data,
                params={"count": limit},
                ttl_hours=24  # Cache for 24 hours
            )
            
            if registrations_data and len(registrations_data) > 0:
                history_text += "\n## Registration History\n"
                history_text += "| Transaction | Action | Timestamp |\n"
                history_text += "|-------------|--------|----------|\n"
                
                for registration in registrations_data:
                    tx_hash = registration.get('tx_hash', 'Unknown')
                    action = registration.get('action', 'Unknown')
                    
                    # Try to get transaction time - cache transaction data
                    tx_data_items = await fetch_and_cache_data(
                        endpoint=f"txs/{tx_hash}",
                        cache_table='cache_block_transactions',
                        key_field='tx_hash',
                        key_value=tx_hash,
                        ttl_hours=48  # Cache for 48 hours as this doesn't change
                    )
                    
                    timestamp = "Unknown"
                    if tx_data_items and len(tx_data_items) > 0 and 'block_time' in tx_data_items[0]:
                        tx_data = tx_data_items[0]
                        timestamp_utc = datetime.fromtimestamp(tx_data['block_time']).strftime('%Y-%m-%d %H:%M:%S UTC')
                        timestamp = timestamp_utc
                    
                    history_text += f"| {tx_hash[:8]}... | {action} | {timestamp} |\n"
            elif history_type == "registrations":
                history_text += "## Registration History\nNo registrations found for this account.\n\n"
        
        # Get withdrawal history if requested
        if history_type in ["all", "withdrawals"]:
            withdrawals_data = await fetch_and_cache_data(
                endpoint=f"accounts/{stake_address}/withdrawals",
                cache_table='cache_account_withdrawals',
                key_field='stake_address',
                key_value=stake_address,
                validator=validate_withdrawal_data,
                params={"count": limit},
                ttl_hours=12  # Cache for 12 hours
            )
            
            if withdrawals_data and len(withdrawals_data) > 0:
                history_text += "\n## Withdrawal History\n"
                history_text += "| Transaction | Amount (ADA) | Timestamp |\n"
                history_text += "|-------------|-------------|----------|\n"
                
                for withdrawal in withdrawals_data:
                    tx_hash = withdrawal.get('tx_hash', 'Unknown')
                    amount = lovelace_to_ada(withdrawal.get('amount', 0))
                    
                    # Try to get transaction time - cache transaction data
                    tx_data_items = await fetch_and_cache_data(
                        endpoint=f"txs/{tx_hash}",
                        cache_table='cache_block_transactions',
                        key_field='tx_hash',
                        key_value=tx_hash,
                        ttl_hours=48  # Cache for 48 hours as this doesn't change
                    )
                    
                    timestamp = "Unknown"
                    if tx_data_items and len(tx_data_items) > 0 and 'block_time' in tx_data_items[0]:
                        tx_data = tx_data_items[0]
                        timestamp_utc = datetime.fromtimestamp(tx_data['block_time']).strftime('%Y-%m-%d %H:%M:%S UTC')
                        timestamp = timestamp_utc
                    
                    history_text += f"| {tx_hash[:8]}... | {amount:.6f} | {timestamp} |\n"
            elif history_type == "withdrawals":
                history_text += "## Withdrawal History\nNo withdrawals found for this account.\n\n"
        
        # Get MIR history if requested
        if history_type in ["all", "mirs"]:
            mirs_data = await fetch_and_cache_data(
                endpoint=f"accounts/{stake_address}/mirs",
                cache_table='cache_account_mirs',
                key_field='stake_address',
                key_value=stake_address,
                validator=validate_withdrawal_data,  # Reuse withdrawal validator
                params={"count": limit},
                ttl_hours=24  # Cache for 24 hours
            )
            
            if mirs_data and len(mirs_data) > 0:
                history_text += "\n## MIR (Move Instantaneous Rewards) History\n"
                history_text += "| Transaction | Amount (ADA) | Type |\n"
                history_text += "|-------------|-------------|------|\n"
                
                for mir in mirs_data:
                    tx_hash = mir.get('tx_hash', 'Unknown')
                    amount = lovelace_to_ada(mir.get('amount', 0))
                    mir_type = mir.get('type', 'Unknown')
                    
                    history_text += f"| {tx_hash[:8]}... | {amount:.6f} | {mir_type} |\n"
            elif history_type == "mirs":
                history_text += "## MIR History\nNo MIR transactions found for this account.\n\n"
                
        return [types.TextContent(type="text", text=history_text)]
        
    except Exception as e:
        logger.error(f"Error in account_history: {e}")
        logger.error(traceback.format_exc())
        return [types.TextContent(
            type="text",
            text=f"An error occurred while retrieving account history: {str(e)}"
        )]

@mcp.tool(name="address_assets_tracker", description="Track assets held at a Cardano address with valuation and history information.")
async def address_assets_tracker(
    address: str, 
    include_valuation: bool = True, 
    include_metadata: bool = True,
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    page_size: int = None,
    summary_only: bool = False,
    filter_policy: str = None,
    chunk_output: bool = None,
    skip_problematic: bool = True,  # Add this parameter
    max_processing_time: int = 60   # Add this parameter
) -> List[types.TextContent]:
    """
    Track assets held at a Cardano address with valuation and history information.
    
    Args:
        address: Cardano address to track assets for
        include_valuation: Whether to include estimated ADA valuation
        include_metadata: Whether to include asset metadata
        page: Page number for pagination
        page_size: Items per page (default: from global config)
        summary_only: Return only a summary of assets without details
        filter_policy: Filter assets by policy ID
        chunk_output: Whether to split large output into chunks
        skip_problematic: Skip assets that cause timeouts or errors
        max_processing_time: Maximum time (seconds) to spend processing assets
        
    Returns:
        Assets tracking information
    """
    try:
        # Get address details with extended information - with caching
        address_data_items = await fetch_and_cache_data(
            endpoint=f"addresses/{address}/extended",
            cache_table='cache_address_assets',
            key_field='address',
            key_value=address,
            ttl_hours=6,  # Cache for 6 hours since assets can change
            timeout=15.0  # Add shorter timeout
        )
        
        if not address_data_items or len(address_data_items) == 0:
            # Check if we got an error response
            if address_data_items and isinstance(address_data_items[0], dict) and 'error' in address_data_items[0]:
                error_info = address_data_items[0]
                return [types.TextContent(
                    type="text",
                    text=f"Error retrieving address information: {address}\nDetails: {error_info.get('details', 'Unknown error')}"
                )]
            else:
                return [types.TextContent(
                    type="text",
                    text=f"Error retrieving address information: {address}"
                )]
        
        address_data = address_data_items[0]
        
        # Generate tracker text
        tracker_text = f"# Asset Tracker for {address}\n\n"
        
        # Extract assets from address data
        assets = []
        ada_balance = 0
        
        if 'amount' in address_data and isinstance(address_data['amount'], list):
            for amount_entry in address_data['amount']:
                if 'unit' in amount_entry and 'quantity' in amount_entry:
                    if amount_entry['unit'] == 'lovelace':
                        ada_balance = int(amount_entry['quantity'])
                    else:
                        assets.append({
                            'unit': amount_entry['unit'],
                            'quantity': int(amount_entry['quantity'])
                        })
        
        tracker_text += f"## Overview\n"
        tracker_text += f"- ADA Balance: {lovelace_to_ada(ada_balance):.6f} ADA\n"
        tracker_text += f"- Total Assets: {len(assets)}\n\n"
        
        if not assets:
            tracker_text += "No native tokens found at this address.\n"
            return [types.TextContent(type="text", text=tracker_text)]
            
        # Get transaction history for asset activity tracking - with caching
        tx_history_items = await fetch_and_cache_data(
            endpoint=f"addresses/{address}/transactions",
            cache_table='cache_address_transactions',
            key_field='address',
            key_value=address,
            validator=validate_transaction_data,
            params={"count": 20, "order": "desc"},
            ttl_hours=6,  # Cache for 6 hours
            timeout=10.0  # Shorter timeout
        )
        
        # Process assets with additional information
        detailed_assets = []
        problematic_assets = []
        processing_start_time = time.time()
        
        # Add timeout tracking
        for asset in assets:
            # Check if we've exceeded the max processing time
            current_time = time.time()
            if current_time - processing_start_time > max_processing_time:
                logger.warning(f"Maximum processing time ({max_processing_time}s) exceeded. Processing {len(detailed_assets)} of {len(assets)} assets.")
                tracker_text += f"\n⚠️ **Note:** Processing time limit reached. Only {len(detailed_assets)} of {len(assets)} assets are displayed.\n"
                break
                
            asset_unit = asset['unit']
            asset_quantity = asset['quantity']
            policy_id = asset_unit[:56] if len(asset_unit) > 56 else asset_unit
            asset_name_hex = asset_unit[56:] if len(asset_unit) > 56 else ''
            
            # Filter by policy if requested
            if filter_policy and policy_id != filter_policy:
                continue
                
            # Initialize detailed asset info
            detailed_asset = {
                'unit': asset_unit,
                'quantity': asset_quantity,
                'policy_id': policy_id,
                'asset_name_hex': asset_name_hex,
                'display_name': asset_unit,  # Default
                'decimals': 0
            }
            
            # Get asset metadata if requested - with caching and shorter timeout
            if include_metadata:
                try:
                    asset_info_items = await fetch_and_cache_data(
                        endpoint=f"assets/{asset_unit}",
                        cache_table='cache_assets',
                        key_field='asset',
                        key_value=asset_unit,
                        ttl_hours=24,  # Cache for 24 hours
                        timeout=5.0,   # Short timeout for asset metadata
                        use_expired_cache=True  # Use expired cache data as fallback
                    )
                    
                    # Check if we received an error response
                    if asset_info_items and isinstance(asset_info_items[0], dict) and 'error' in asset_info_items[0]:
                        if skip_problematic:
                            logger.warning(f"Skipping problematic asset {asset_unit}: {asset_info_items[0]['error']}")
                            problematic_assets.append({
                                'unit': asset_unit, 
                                'error': asset_info_items[0]['error']
                            })
                            continue
                        else:
                            # Include the asset but mark it as having failed metadata lookup
                            detailed_asset['metadata_error'] = asset_info_items[0]['error']
                    elif asset_info_items and len(asset_info_items) > 0:
                        asset_info = asset_info_items[0]
                        
                        # Check if this is from expired cache
                        if asset_info.get('_from_expired_cache'):
                            detailed_asset['from_expired_cache'] = True
                        
                        # Extract display name from metadata
                        if 'onchain_metadata' in asset_info and asset_info['onchain_metadata']:
                            if 'name' in asset_info['onchain_metadata']:
                                detailed_asset['display_name'] = asset_info['onchain_metadata']['name']
                            if 'description' in asset_info['onchain_metadata']:
                                detailed_asset['description'] = asset_info['onchain_metadata']['description']
                        elif 'metadata' in asset_info and asset_info['metadata']:
                            if 'name' in asset_info['metadata']:
                                detailed_asset['display_name'] = asset_info['metadata']['name']
                            if 'description' in asset_info['metadata']:
                                detailed_asset['description'] = asset_info['metadata']['description']
                            if 'decimals' in asset_info['metadata']:
                                detailed_asset['decimals'] = int(asset_info['metadata']['decimals'])
                        
                        # Add fingerprint if available
                        if 'fingerprint' in asset_info:
                            detailed_asset['fingerprint'] = asset_info['fingerprint']
                        
                        # Add mint/burn info if available
                        if 'mint_or_burn_count' in asset_info:
                            detailed_asset['mint_or_burn_count'] = asset_info['mint_or_burn_count']
                        
                        # Add initial mint transaction
                        if 'initial_mint_tx_hash' in asset_info:
                            detailed_asset['initial_mint_tx_hash'] = asset_info['initial_mint_tx_hash']
                except Exception as asset_error:
                    logger.error(f"Error processing asset {asset_unit}: {asset_error}")
                    if skip_problematic:
                        problematic_assets.append({
                            'unit': asset_unit, 
                            'error': str(asset_error)
                        })
                        continue
                    else:
                        detailed_asset['error'] = str(asset_error)
            
            # Calculate adjusted quantity with decimals
            if 'decimals' in detailed_asset and detailed_asset['decimals'] > 0:
                detailed_asset['adjusted_quantity'] = detailed_asset['quantity'] / (10 ** detailed_asset['decimals'])
            
            # Add asset to detailed list
            detailed_assets.append(detailed_asset)
        
        # If no assets match the filter or all were problematic
        if (filter_policy and not detailed_assets) or (assets and not detailed_assets):
            error_message = ""
            if filter_policy:
                error_message = f"No assets found with policy ID: {filter_policy}\n"
            else:
                error_message = "All assets encountered errors during processing.\n"
                
            if problematic_assets:
                error_message += f"\n{len(problematic_assets)} assets were problematic:\n"
                # Show sample of errors (limit to 5)
                for i, asset in enumerate(problematic_assets[:5]):
                    error_message += f"- {asset['unit']}: {asset['error']}\n"
                if len(problematic_assets) > 5:
                    error_message += f"- ... and {len(problematic_assets) - 5} more\n"
                
                error_message += "\nTry using summary_only=True to get a basic summary instead."
                
            tracker_text += error_message
            return [types.TextContent(type="text", text=tracker_text)]
        
        # Sort assets by quantity (highest first)
        detailed_assets.sort(key=lambda x: x['quantity'], reverse=True)
        
        # If summary_only is True, return just the summary
        if summary_only:
            asset_summary = summarize_assets(detailed_assets)
            tracker_text += f"## Asset Summary\n"
            tracker_text += f"- Total Assets: {asset_summary['total_assets']}\n"
            tracker_text += f"- Unique Policies: {asset_summary['unique_policies']}\n"
            
            if problematic_assets:
                tracker_text += f"- Problematic Assets: {len(problematic_assets)}\n"
            
            if asset_summary['top_policies']:
                tracker_text += "\n## Top Asset Policies\n"
                for policy in asset_summary['top_policies']:
                    tracker_text += f"- {policy['policy_id']}: {policy['asset_count']} assets\n"
                
                if asset_summary['has_more']:
                    tracker_text += "\nUse page parameters to view additional assets.\n"
            
            return [types.TextContent(type="text", text=tracker_text)]
        
        # Apply pagination to the assets list
        effective_page_size = get_config_value("page_size", page_size)
        paginator = PaginatedResponse(detailed_assets, effective_page_size)
        page_data = paginator.get_page(page)
        paged_assets = page_data["items"]
        
        # Update the tracker text to include pagination info
        if page_data['total_pages'] > 1:
            tracker_text += f"Showing page {page_data['page']} of {page_data['total_pages']} "
            tracker_text += f"({page_data['total_items']} total assets)\n\n"
        
        # Note about problematic assets
        if problematic_assets:
            tracker_text += f"⚠️ **Note:** {len(problematic_assets)} problematic assets were skipped due to API errors or timeouts.\n\n"
        
        # Display assets table
        tracker_text += "## Assets Summary\n"
        tracker_text += "| Asset | Policy ID | Quantity | Adjusted Quantity |\n"
        tracker_text += "|-------|-----------|----------|-------------------|\n"
        
        for asset in paged_assets:
            display_name = asset.get('display_name', asset['unit'])
            policy_id = asset['policy_id']
            quantity = asset['quantity']
            
            # Include adjusted quantity if available
            adjusted_quantity = ""
            if 'adjusted_quantity' in asset:
                adjusted_quantity = f"{asset['adjusted_quantity']}"
            
            # Mark assets from expired cache
            if asset.get('from_expired_cache'):
                display_name = f"{display_name}*"
                
            # Mark assets with errors
            if 'error' in asset or 'metadata_error' in asset:
                display_name = f"{display_name}†"
            
            tracker_text += f"| {display_name} | {policy_id[:8]}... | {quantity} | {adjusted_quantity} |\n"
        
        # Add legend for special markers if needed
        if any(asset.get('from_expired_cache') for asset in paged_assets):
            tracker_text += "\n\\* Asset information from expired cache\n"
        if any('error' in asset or 'metadata_error' in asset for asset in paged_assets):
            tracker_text += "\n† Asset information retrieval encountered errors\n"
        
        # Include asset metadata if requested
        if include_metadata:
            tracker_text += "\n## Asset Details\n"
            
            for asset in paged_assets:
                display_name = asset.get('display_name', asset['unit'])
                
                tracker_text += f"\n### {display_name}\n"
                tracker_text += f"- Asset ID: {asset['unit']}\n"
                tracker_text += f"- Policy ID: {asset['policy_id']}\n"
                
                if 'asset_name_hex' in asset and asset['asset_name_hex']:
                    tracker_text += f"- Asset Name (hex): {asset['asset_name_hex']}\n"
                
                if 'fingerprint' in asset:
                    tracker_text += f"- Fingerprint: {asset['fingerprint']}\n"
                
                if 'description' in asset:
                    tracker_text += f"- Description: {asset['description']}\n"
                
                tracker_text += f"- Quantity: {asset['quantity']}\n"
                
                if 'decimals' in asset and asset['decimals'] > 0:
                    tracker_text += f"- Decimals: {asset['decimals']}\n"
                    tracker_text += f"- Adjusted Quantity: {asset['adjusted_quantity']}\n"
                
                if 'initial_mint_tx_hash' in asset:
                    tracker_text += f"- Initial Mint TX: {asset['initial_mint_tx_hash']}\n"
                
                if 'mint_or_burn_count' in asset:
                    tracker_text += f"- Mint/Burn Count: {asset['mint_or_burn_count']}\n"
                    
                # Show any errors if present
                if 'error' in asset:
                    tracker_text += f"- ⚠️ Error: {asset['error']}\n"
                if 'metadata_error' in asset:
                    tracker_text += f"- ⚠️ Metadata Error: {asset['metadata_error']}\n"
                if asset.get('from_expired_cache'):
                    tracker_text += f"- ⚠️ Note: Using expired cached data\n"
        
        # Include valuation information if requested
        if include_valuation:
            tracker_text += "\n## Asset Valuation (Estimated)\n"
            
            # Note: For a production implementation, this would connect to a price API
            # Since we don't have that, we'll add a placeholder with an explanation
            
            tracker_text += "Valuation information would normally include:\n"
            tracker_text += "- Estimated ADA value of each asset\n"
            tracker_text += "- Total portfolio value in ADA and USD\n"
            tracker_text += "- Price history trends\n\n"
            tracker_text += "To implement actual valuation, a price oracle or exchange API would be required.\n"
        
        # Add pagination info
        if page_data['total_pages'] > 1:
            tracker_text += format_pagination_info(page_data)
            
        # Add tips for handling large asset collections
        if len(assets) > 100 or problematic_assets:
            tracker_text += "\n## Performance Tips\n"
            tracker_text += "This address has a large number of assets. To improve performance:\n"
            tracker_text += "- Use summary_only=True to get just an overview\n"
            tracker_text += "- Use filter_policy parameter to focus on specific tokens\n"
            tracker_text += "- Set include_metadata=False to skip detailed metadata lookup\n"
            if problematic_assets:
                tracker_text += "- Some assets are causing timeouts. These have been skipped automatically.\n"
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        results = []
        
        if should_chunk:
            chunks = chunk_text(tracker_text)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=tracker_text))
            
        return results
        
    except Exception as e:
        # Handle unexpected errors gracefully
        logger.error(f"Error in address_assets_tracker: {e}")
        logger.error(traceback.format_exc())
        
        error_text = f"# Error Tracking Assets\n\n"
        error_text += f"An error occurred while tracking assets for address {address}:\n"
        error_text += f"{str(e)}\n\n"
        error_text += "To work around this issue, you can try:\n"
        error_text += "1. Using summary_only=True to get just a summary\n"
        error_text += "2. Using filter_policy parameter to focus on specific assets\n"
        error_text += "3. Setting skip_problematic=True to skip assets that cause errors\n"
        error_text += "4. Increasing max_processing_time if needed\n\n"
        error_text += "Example:\n"
        error_text += f"address_assets_tracker(address=\"{address}\", summary_only=True, skip_problematic=True)\n"
        
        return [types.TextContent(type="text", text=error_text)]

@mcp.tool(name="account_assets_summary", description="View all assets held across addresses for a Cardano stake address.")
async def account_assets_summary(stake_address: str, include_details: bool = True) -> List[types.TextContent]:
    """
    View all assets held across addresses for a Cardano stake address.
    
    Args:
        stake_address: Cardano stake address
        include_details: Whether to include detailed asset information
        
    Returns:
        Assets summary information
    """
    try:
        # Get all addresses associated with this stake address
        addresses_data = await fetch_and_cache_data(
            endpoint=f"accounts/{stake_address}/addresses",
            cache_table='cache_account_addresses',
            key_field='stake_address',
            key_value=stake_address,
            validator=validate_address_data,
            params={"count": 100},
            ttl_hours=12  # Cache for 12 hours
        )
        
        if not addresses_data or len(addresses_data) == 0:
            return [types.TextContent(
                type="text",
                text=f"No addresses found for stake address {stake_address}."
            )]
        
        # Generate summary text
        summary_text = f"# Assets Summary for {stake_address}\n\n"
        
        # Track assets across all addresses
        ada_total = 0
        assets_summary = {}
        addresses_count = len(addresses_data)
        
        summary_text += f"## Overview\n"
        summary_text += f"- Total Addresses: {addresses_count}\n\n"
        
        for address_info in addresses_data:
            address = address_info.get('address')
            if address:
                # Get assets at this address - cache each address's assets separately
                address_assets_items = await fetch_and_cache_data(
                    endpoint=f"addresses/{address}/extended",
                    cache_table='cache_address_assets',
                    key_field='address',
                    key_value=address,
                    ttl_hours=6  # Cache for 6 hours since assets can change
                )
                
                if address_assets_items and len(address_assets_items) > 0:
                    address_assets = address_assets_items[0]
                    
                    # Extract ADA amount and other assets
                    if 'amount' in address_assets and isinstance(address_assets['amount'], list):
                        for amount_entry in address_assets['amount']:
                            if 'unit' in amount_entry and amount_entry['unit'] == 'lovelace':
                                ada_total += int(amount_entry.get('quantity', 0))
                            else:
                                # Non-lovelace assets (tokens)
                                asset_unit = amount_entry.get('unit', 'unknown')
                                asset_quantity = int(amount_entry.get('quantity', 0))
                                
                                if asset_unit in assets_summary:
                                    assets_summary[asset_unit]['quantity'] += asset_quantity
                                    if address not in assets_summary[asset_unit]['addresses']:
                                        assets_summary[asset_unit]['addresses'].append(address)
                                else:
                                    # Initialize new asset entry
                                    assets_summary[asset_unit] = {
                                        'quantity': asset_quantity,
                                        'policy_id': asset_unit[:56] if len(asset_unit) > 56 else asset_unit,
                                        'asset_name': asset_unit[56:] if len(asset_unit) > 56 else '',
                                        'addresses': [address]
                                    }
        
        # Add ADA total to summary
        summary_text += f"## ADA Balance\n"
        summary_text += f"- Total ADA: {lovelace_to_ada(ada_total):.6f}\n\n"
        
        # Add assets summary
        if assets_summary:
            summary_text += f"## Native Tokens ({len(assets_summary)} unique assets)\n"
            
            # Get detailed asset information if requested
            if include_details:
                for asset_unit, details in assets_summary.items():
                    # Try to get asset metadata - cache asset details separately
                    asset_details_items = await fetch_and_cache_data(
                        endpoint=f"assets/{asset_unit}",
                        cache_table='cache_assets',
                        key_field='asset',
                        key_value=asset_unit,
                        ttl_hours=24  # Cache for 24 hours
                    )
                    
                    if asset_details_items and len(asset_details_items) > 0:
                        asset_details = asset_details_items[0]
                        details['name'] = asset_details.get('onchain_metadata', {}).get('name', asset_unit)
                        details['description'] = asset_details.get('onchain_metadata', {}).get('description', 'No description')
                        details['decimals'] = asset_details.get('metadata', {}).get('decimals', 0)
                        
                        # Add fingerprint if available
                        if 'fingerprint' in asset_details:
                            details['fingerprint'] = asset_details['fingerprint']
                    else:
                        details['name'] = asset_unit
                        details['description'] = 'No description'
                        details['decimals'] = 0
            
            # Display assets
            summary_text += "| Asset | Policy ID | Quantity | Addresses |\n"
            summary_text += "|-------|-----------|----------|----------|\n"
            
            for asset_unit, details in assets_summary.items():
                asset_name = details.get('name', asset_unit)
                policy_id = details['policy_id']
                quantity = details['quantity']
                addresses_count = len(details['addresses'])
                
                summary_text += f"| {asset_name} | {policy_id[:8]}... | {quantity} | {addresses_count} |\n"
            
            # If detailed info requested, add more details for each asset
            if include_details:
                summary_text += "\n## Asset Details\n"
                
                for asset_unit, details in assets_summary.items():
                    summary_text += f"\n### {details.get('name', asset_unit)}\n"
                    summary_text += f"- Full Asset ID: {asset_unit}\n"
                    summary_text += f"- Policy ID: {details['policy_id']}\n"
                    summary_text += f"- Asset Name: {details.get('asset_name', 'N/A')}\n"
                    summary_text += f"- Description: {details.get('description', 'No description')}\n"
                    summary_text += f"- Quantity: {details['quantity']}\n"
                    
                    if 'decimals' in details and details['decimals']:
                        adjusted_quantity = details['quantity'] / (10 ** int(details['decimals']))
                        summary_text += f"- Adjusted Quantity (with decimals): {adjusted_quantity}\n"
                    
                    if 'fingerprint' in details:
                        summary_text += f"- Fingerprint: {details['fingerprint']}\n"
                    
                    # List addresses containing this asset (first 5)
                    if details['addresses']:
                        addresses_str = '\n  - ' + '\n  - '.join(details['addresses'][:5])
                        if len(details['addresses']) > 5:
                            addresses_str += f"\n  - ... and {len(details['addresses']) - 5} more"
                        summary_text += f"- Addresses: {addresses_str}\n"
        else:
            summary_text += "## Native Tokens\nNo native tokens found for this account.\n"
        
        return [types.TextContent(type="text", text=summary_text)]
        
    except Exception as e:
        logger.error(f"Error in account_assets_summary: {e}")
        logger.error(traceback.format_exc())
        return [types.TextContent(
            type="text",
            text=f"An error occurred while retrieving assets summary: {str(e)}"
        )]


@mcp.tool(
    name="address_explorer",
    description="""Explore Cardano payment address details, balances, and activity.

    Use this tool when users ask about:
    - Payment address balances (ADA and tokens)
    - Address UTXOs and available funds
    - Address transaction history
    - Tokens held at an address
    - Address activity and usage

    IMPORTANT: This is for payment addresses (addr1...), not stake addresses (stake1...).
    For stake addresses, use account_overview instead.

    Example Queries:
    - "What is the balance of address addr1..."
    - "Show me UTXOs for this address"
    - "What tokens does this address hold?"
    - "Get transaction history for addr1..."

    Example Input:
    address: "addr1qxy...xyz"
    include_utxos: true
    include_transactions: true
    transactions_limit: 10

    Returns: Address details with balance, UTXOs, transactions, and token holdings.
    """
)
async def address_explorer(
    address: str,
    include_utxos: bool = True,
    include_transactions: bool = True,
    transactions_limit: int = 10,
    skip_problematic: bool = True,   # New parameter
    max_processing_time: int = 60,   # New parameter
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),                   # New parameter
    page_size: int = None,           # New parameter
    chunk_output: bool = None        # New parameter
) -> List[types.TextContent]:
    """
    Explore Cardano address details, balances and activity.
    
    Args:
        address: Cardano address to explore
        include_utxos: Whether to include UTXOs information
        include_transactions: Whether to include recent transactions
        transactions_limit: Maximum number of transactions to include
        skip_problematic: Skip sections that cause timeouts or errors
        max_processing_time: Maximum time (seconds) to spend processing
        page: Page number for pagination
        page_size: Items per page (default: from global config)
        chunk_output: Whether to split large output into chunks
        
    Returns:
        Address details and activity information
    """
    try:
        processing_start_time = time.time()
        
        # Get address details with caching
        address_data = await fetch_and_cache_data(
            endpoint=f"addresses/{address}",
            cache_table='cache_addresses',
            key_field='address',
            key_value=address,
            validator=validate_address_data,
            ttl_hours=6,  # Cache for 6 hours
            use_expired_cache=True  # Use expired cache as fallback
        )
        
        if not address_data or len(address_data) == 0:
            return [types.TextContent(
                type="text",
                text=f"Error retrieving address information: {address}"
            )]
        
        # Get extended address info for more details
        extended_data = await fetch_and_cache_data(
            endpoint=f"addresses/{address}/extended",
            cache_table='cache_address_details',
            key_field='address',
            key_value=address,
            ttl_hours=6,  # Cache for 6 hours
            use_expired_cache=True  # Use expired cache as fallback
        )
        
        # Generate explorer text
        explorer_text = f"# Address Explorer: {address}\n\n"
        
        explorer_text += "## Basic Information\n"
        
        # Check if the address is associated with a stake address
        stake_address = None
        if extended_data and len(extended_data) > 0 and 'stake_address' in extended_data[0]:
            stake_address = extended_data[0].get('stake_address')
            if stake_address:
                explorer_text += f"- Stake Address: {stake_address}\n"
            
            # Extract address type if available
            address_type = "Unknown"
            if 'script' in extended_data[0]:
                address_type = "Script Address" if extended_data[0]['script'] else "Normal Address"
            explorer_text += f"- Address Type: {address_type}\n"
        
        # Extract balance information
        ada_balance = 0
        assets = []
        
        if extended_data and len(extended_data) > 0 and 'amount' in extended_data[0]:
            for amount_entry in extended_data[0]['amount']:
                if 'unit' in amount_entry and 'quantity' in amount_entry:
                    if amount_entry['unit'] == 'lovelace':
                        ada_balance = int(amount_entry['quantity'])
                    else:
                        assets.append({
                            'unit': amount_entry['unit'],
                            'quantity': int(amount_entry['quantity'])
                        })
        
        explorer_text += f"- ADA Balance: {lovelace_to_ada(ada_balance):.6f} ADA\n"
        explorer_text += f"- Native Tokens: {len(assets)}\n\n"
        
        # Display assets if any
        if assets:
            # Check processing time
            current_time = time.time()
            if current_time - processing_start_time > max_processing_time:
                explorer_text += "⚠️ Processing time limit reached. Assets section skipped.\n"
            else:
                explorer_text += "## Native Tokens\n"
                
                # Apply pagination to assets if needed
                effective_page_size = get_config_value("page_size", page_size)
                paginator = PaginatedResponse(assets, effective_page_size)
                page_data = paginator.get_page(page)
                
                if page_data['total_pages'] > 1:
                    explorer_text += f"Showing page {page_data['page']} of {page_data['total_pages']} "
                    explorer_text += f"(total assets: {page_data['total_items']})\n\n"
                
                explorer_text += "| Asset | Policy ID | Quantity |\n"
                explorer_text += "|-------|-----------|----------|\n"
                
                problematic_assets = 0
                for asset in page_data['items']:
                    try:
                        # Check processing time for each asset
                        current_time = time.time()
                        if current_time - processing_start_time > max_processing_time:
                            explorer_text += "\n⚠️ Processing time limit reached. Not all assets are displayed.\n"
                            break
                            
                        asset_unit = asset['unit']
                        policy_id = asset_unit[:56] if len(asset_unit) > 56 else asset_unit
                        asset_name_hex = asset_unit[56:] if len(asset_unit) > 56 else ''
                        
                        # Try to get asset details from cache or API
                        asset_details_items = await fetch_and_cache_data(
                            endpoint=f"assets/{asset_unit}",
                            cache_table='cache_assets',
                            key_field='asset',
                            key_value=asset_unit,
                            ttl_hours=24,  # Cache for 24 hours
                            timeout=5.0,   # Shorter timeout
                            use_expired_cache=True  # Use expired cache as fallback
                        )
                        
                        asset_display_name = asset_unit
                        
                        if asset_details_items and len(asset_details_items) > 0:
                            asset_details = asset_details_items[0]
                            if 'onchain_metadata' in asset_details and asset_details['onchain_metadata'] and 'name' in asset_details['onchain_metadata']:
                                asset_display_name = asset_details['onchain_metadata']['name']
                            elif 'metadata' in asset_details and asset_details['metadata'] and 'name' in asset_details['metadata']:
                                asset_display_name = asset_details['metadata']['name']
                        
                        explorer_text += f"| {asset_display_name} | {policy_id[:8]}... | {asset['quantity']} |\n"
                    except Exception as e:
                        logger.error(f"Error processing asset {asset.get('unit', 'unknown')}: {e}")
                        problematic_assets += 1
                        
                        if not skip_problematic:
                            raise
                
                # Add information about problematic assets
                if problematic_assets > 0:
                    explorer_text += f"\n⚠️ {problematic_assets} assets were skipped due to errors.\n"
                    
                # Add pagination information if needed
                if page_data['total_pages'] > 1:
                    explorer_text += format_pagination_info(page_data)
        
        # Include UTXOs if requested
        if include_utxos:
            try:
                # Check processing time
                current_time = time.time()
                if current_time - processing_start_time > max_processing_time:
                    explorer_text += "\n⚠️ Processing time limit reached. UTXOs section skipped.\n"
                else:
                    utxos_data = await fetch_and_cache_data(
                        endpoint=f"addresses/{address}/utxos",
                        cache_table='cache_address_utxos',
                        key_field='address',
                        key_value=address,
                        validator=validate_utxo_data,
                        ttl_hours=2,  # Cache for 2 hours since UTXOs change frequently
                        use_expired_cache=True  # Use expired cache as fallback
                    )
                    
                    if utxos_data and len(utxos_data) > 0:
                        # Apply pagination to UTXOs if needed
                        effective_page_size = get_config_value("page_size", page_size)
                        paginator = PaginatedResponse(utxos_data, effective_page_size)
                        page_data = paginator.get_page(page)
                        
                        explorer_text += "\n## UTXOs\n"
                        
                        if page_data['total_pages'] > 1:
                            explorer_text += f"Showing page {page_data['page']} of {page_data['total_pages']} "
                            explorer_text += f"(total UTXOs: {page_data['total_items']})\n\n"
                        
                        explorer_text += "| TX Hash | Index | ADA | Assets | Data Hash |\n"
                        explorer_text += "|---------|-------|-----|--------|----------|\n"
                        
                        problematic_utxos = 0
                        for utxo in page_data['items']:
                            try:
                                # Check processing time for each UTXO
                                current_time = time.time()
                                if current_time - processing_start_time > max_processing_time:
                                    explorer_text += "\n⚠️ Processing time limit reached. Not all UTXOs are displayed.\n"
                                    break
                                    
                                tx_hash = utxo.get('tx_hash', 'Unknown')
                                tx_index = utxo.get('output_index', 0)
                                
                                # Extract ADA amount and count assets
                                ada = 0
                                asset_count = 0
                                
                                if 'amount' in utxo:
                                    for amount_entry in utxo['amount']:
                                        if amount_entry['unit'] == 'lovelace':
                                            ada = lovelace_to_ada(amount_entry['quantity'])
                                        else:
                                            asset_count += 1
                                
                                data_hash = utxo.get('data_hash', 'None')
                                if data_hash == None:
                                    data_hash = 'None'
                                
                                explorer_text += f"| {tx_hash[:8]}... | {tx_index} | {ada:.6f} | {asset_count} | {data_hash} |\n"
                            except Exception as e:
                                logger.error(f"Error processing UTXO {utxo.get('tx_hash', 'unknown')}: {e}")
                                problematic_utxos += 1
                                
                                if not skip_problematic:
                                    raise
                        
                        # Add information about problematic UTXOs
                        if problematic_utxos > 0:
                            explorer_text += f"\n⚠️ {problematic_utxos} UTXOs were skipped due to errors.\n"
                            
                        # Add pagination information if needed
                        if page_data['total_pages'] > 1:
                            explorer_text += format_pagination_info(page_data)
                    else:
                        explorer_text += "\n## UTXOs\nNo UTXOs found for this address.\n"
            except Exception as e:
                logger.error(f"Error retrieving UTXOs: {e}")
                if skip_problematic:
                    explorer_text += "\n## UTXOs\nError retrieving UTXOs information.\n"
                else:
                    raise
        
        # Include transactions if requested
        if include_transactions:
            try:
                # Check processing time
                current_time = time.time()
                if current_time - processing_start_time > max_processing_time:
                    explorer_text += "\n⚠️ Processing time limit reached. Transactions section skipped.\n"
                else:
                    txs_data = await fetch_and_cache_data(
                        endpoint=f"addresses/{address}/transactions",
                        cache_table='cache_address_transactions',
                        key_field='address',
                        key_value=address,
                        validator=validate_transaction_data,
                        params={"count": transactions_limit, "order": "desc"},
                        ttl_hours=6,  # Cache for 6 hours
                        use_expired_cache=True  # Use expired cache as fallback
                    )
                    
                    if txs_data and len(txs_data) > 0:
                        explorer_text += "\n## Recent Transactions\n"
                        explorer_text += "| TX Hash | Block | Time | Fees (ADA) |\n"
                        explorer_text += "|---------|-------|------|------------|\n"
                        
                        problematic_txs = 0
                        for tx_info in txs_data:
                            try:
                                # Check processing time for each transaction
                                current_time = time.time()
                                if current_time - processing_start_time > max_processing_time:
                                    explorer_text += "\n⚠️ Processing time limit reached. Not all transactions are displayed.\n"
                                    break
                                    
                                tx_hash = tx_info.get('tx_hash', 'Unknown')
                                
                                # Get detailed transaction info - cache each transaction separately
                                tx_data_items = await fetch_and_cache_data(
                                    endpoint=f"txs/{tx_hash}",
                                    cache_table='cache_block_transactions',
                                    key_field='tx_hash',
                                    key_value=tx_hash,
                                    ttl_hours=48,  # Cache for 48 hours as this doesn't change
                                    timeout=5.0,    # Shorter timeout
                                    use_expired_cache=True  # Use expired cache as fallback
                                )
                                
                                block = tx_info.get('block_height', 'Unknown')
                                timestamp = "Unknown"
                                fees = 0
                                
                                if tx_data_items and len(tx_data_items) > 0:
                                    tx_data = tx_data_items[0]
                                    if 'block_time' in tx_data:
                                        timestamp_utc = datetime.fromtimestamp(tx_data['block_time']).strftime('%Y-%m-%d %H:%M:%S UTC')
                                        timestamp = timestamp_utc
                                    
                                    if 'fees' in tx_data:
                                        fees = lovelace_to_ada(tx_data['fees'])
                                
                                explorer_text += f"| {tx_hash[:8]}... | {block} | {timestamp} | {fees:.6f} |\n"
                            except Exception as e:
                                logger.error(f"Error processing transaction {tx_hash}: {e}")
                                problematic_txs += 1
                                
                                if skip_problematic:
                                    explorer_text += f"| {tx_hash[:8]}... | {block} | Error | Error |\n"
                                else:
                                    raise
                        
                        # Add information about problematic transactions
                        if problematic_txs > 0 and not skip_problematic:
                            explorer_text += f"\n⚠️ {problematic_txs} transactions were skipped due to errors.\n"
                    else:
                        explorer_text += "\n## Recent Transactions\nNo transactions found for this address.\n"
            except Exception as e:
                logger.error(f"Error retrieving transactions: {e}")
                if skip_problematic:
                    explorer_text += "\n## Recent Transactions\nError retrieving transaction information.\n"
                else:
                    raise
        
        # If we have a stake address, add a link to account information
        if stake_address:
            explorer_text += "\n## Related Account Information\n"
            explorer_text += f"This address belongs to stake account {stake_address}. "
            explorer_text += "Use the `account_overview` tool with this stake address to see more information about the full account."
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        results = []
        
        if should_chunk:
            chunks = chunk_text(explorer_text)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=explorer_text))
            
        return results
        
    except Exception as e:
        logger.error(f"Error in address_explorer: {e}")
        logger.error(traceback.format_exc())
        
        error_text = f"# Error Exploring Address\n\n"
        error_text += f"An error occurred while exploring address: {str(e)}\n\n"
        error_text += "To work around this issue, you can try:\n"
        error_text += "1. Setting skip_problematic=True to handle errors gracefully\n"
        error_text += "2. Disabling specific sections (include_utxos=False, include_transactions=False)\n"
        error_text += "3. Reducing transactions_limit to retrieve fewer transactions\n"
        error_text += "4. Increasing max_processing_time if needed\n\n"
        
        return [types.TextContent(type="text", text=error_text)]

@mcp.tool(name="address_utxo_browser", description="View all UTXOs at a Cardano address with detailed information.")
async def address_utxo_browser(
    address: str, 
    include_assets: bool = True, 
    include_datum: bool = True,
    lazy_load: bool = False,
    utxo_hash: str = None,
    output_index: int = None,
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    page_size: int = None,
    chunk_output: bool = None
) -> List[types.TextContent]:
    """
    View all UTXOs at a Cardano address with detailed information.
    
    Args:
        address: Cardano address to explore
        include_assets: Whether to include detailed asset information
        include_datum: Whether to include datum information
        lazy_load: Load UTXOs one at a time on demand
        utxo_hash: Specific UTXO hash to load (with lazy_load)
        output_index: Output index for the UTXO (use with utxo_hash)
        page: Page number for pagination
        page_size: UTXOs per page (default: from global config)
        chunk_output: Whether to split large output into chunks
        
    Returns:
        Detailed UTXO information
    """
    try:
        # Get UTXOs for the address with caching
        utxos_data = await fetch_and_cache_data(
            endpoint=f"addresses/{address}/utxos",
            cache_table='cache_address_utxos',
            key_field='address',
            key_value=address,
            validator=validate_utxo_data,
            ttl_hours=2  # Cache for 2 hours since UTXOs change frequently
        )
        
        if not utxos_data or len(utxos_data) == 0:
            return [types.TextContent(
                type="text",
                text=f"No UTXOs found for address {address}."
            )]
            
        # If lazy_load is True and a specific UTXO is requested, return only that one
        if lazy_load and utxo_hash:
            # Find the specific UTXO
            specific_utxos = []
            for utxo in utxos_data:
                if utxo.get('tx_hash') == utxo_hash:
                    if output_index is not None:
                        if utxo.get('output_index') == output_index:
                            specific_utxos.append(utxo)
                    else:
                        specific_utxos.append(utxo)
            
            if not specific_utxos:
                output_info = f" with index {output_index}" if output_index is not None else ""
                return [types.TextContent(
                    type="text",
                    text=f"UTXO with hash {utxo_hash}{output_info} not found at address {address}."
                )]
            
            # Generate detailed info for just this UTXO
            utxo_detail_text = f"# UTXO Details\n\n"
            
            for utxo in specific_utxos:
                tx_hash = utxo.get('tx_hash', 'Unknown')
                tx_index = utxo.get('output_index', 0)
                
                utxo_detail_text += f"## UTXO: {tx_hash}#{tx_index}\n"
                
                # Basic information
                utxo_detail_text += "### Basic Information\n"
                utxo_detail_text += f"- Transaction: {tx_hash}\n"
                utxo_detail_text += f"- Output Index: {tx_index}\n"
                utxo_detail_text += f"- Block: {utxo.get('block', 'Unknown')}\n"
                
                # Data hash information if present
                data_hash = utxo.get('data_hash')
                if data_hash:
                    utxo_detail_text += f"- Data Hash: {data_hash}\n"
                
                # Reference script if present
                reference_script = utxo.get('reference_script_hash')
                if reference_script:
                    utxo_detail_text += f"- Reference Script: {reference_script}\n"
                    
                    # Try to get script details - cache script data
                    script_data_items = await fetch_and_cache_data(
                        endpoint=f"scripts/{reference_script}",
                        cache_table='cache_scripts',
                        key_field='script_hash',
                        key_value=reference_script,
                        ttl_hours=48  # Cache for 48 hours as scripts don't change
                    )
                    
                    if script_data_items and len(script_data_items) > 0:
                        script_data = script_data_items[0]
                        utxo_detail_text += f"  - Script Type: {script_data.get('type', 'Unknown')}\n"
                        utxo_detail_text += f"  - Script Size: {script_data.get('serialised_size', 'Unknown')} bytes\n"
                
                # Datum information if requested and present
                if include_datum:
                    inline_datum = utxo.get('inline_datum')
                    if inline_datum:
                        utxo_detail_text += f"- Inline Datum: {inline_datum}\n"
                
                # UTXO value
                if 'amount' in utxo:
                    ada_amount = 0
                    asset_entries = []
                    
                    for amount_entry in utxo['amount']:
                        if amount_entry['unit'] == 'lovelace':
                            ada_amount = int(amount_entry['quantity'])
                        else:
                            asset_entries.append(amount_entry)
                    
                    utxo_detail_text += f"\n### Value\n"
                    utxo_detail_text += f"- ADA: {lovelace_to_ada(ada_amount):.6f}\n"
                    utxo_detail_text += f"- Native Tokens: {len(asset_entries)}\n"
                    
                    # Include detailed asset information if requested
                    if include_assets and asset_entries:
                        utxo_detail_text += "\n#### Asset Details\n"
                        utxo_detail_text += "| Asset | Policy ID | Name | Quantity |\n"
                        utxo_detail_text += "|-------|-----------|------|----------|\n"
                        
                        for asset in asset_entries:
                            asset_unit = asset['unit']
                            policy_id = asset_unit[:56] if len(asset_unit) > 56 else asset_unit
                            asset_name_hex = asset_unit[56:] if len(asset_unit) > 56 else ''
                            quantity = int(asset['quantity'])
                            
                            # Try to get human-readable asset name
                            display_name = asset_name_hex
                            
                            asset_details_items = await fetch_and_cache_data(
                                endpoint=f"assets/{asset_unit}",
                                cache_table='cache_assets',
                                key_field='asset',
                                key_value=asset_unit,
                                ttl_hours=24  # Cache for 24 hours
                            )
                            
                            if asset_details_items and len(asset_details_items) > 0:
                                asset_details = asset_details_items[0]
                                # Check for onchain metadata first
                                if 'onchain_metadata' in asset_details and asset_details['onchain_metadata']:
                                    if 'name' in asset_details['onchain_metadata']:
                                        display_name = asset_details['onchain_metadata']['name']
                                # Fall back to off-chain metadata
                                elif 'metadata' in asset_details and asset_details['metadata']:
                                    if 'name' in asset_details['metadata']:
                                        display_name = asset_details['metadata']['name']
                                
                                # Use fingerprint if available and no name found
                                if display_name == asset_name_hex and 'fingerprint' in asset_details:
                                    display_name = asset_details['fingerprint']
                            
                            utxo_detail_text += f"| {display_name} | {policy_id[:8]}... | {asset_name_hex[:8]}... | {quantity} |\n"
            
            # Return the details for the specific UTXO(s)
            return [types.TextContent(type="text", text=utxo_detail_text)]
        
        # If lazy_load is True but no specific UTXO requested, return a summary list
        if lazy_load and not utxo_hash:
            summary_text = f"# UTXO Summary for {address}\n\n"
            summary_text += f"Total UTXOs: {len(utxos_data)}\n\n"
            summary_text += "## Available UTXOs\n"
            summary_text += "| # | TX Hash | Index | ADA |\n"
            summary_text += "|---|---------|-------|-----|\n"
            
            for i, utxo in enumerate(utxos_data):
                tx_hash = utxo.get('tx_hash', 'Unknown')
                tx_index = utxo.get('output_index', 0)
                
                # Extract ADA amount
                ada = 0
                if 'amount' in utxo:
                    for amount_entry in utxo['amount']:
                        if amount_entry['unit'] == 'lovelace':
                            ada = lovelace_to_ada(amount_entry['quantity'])
                
                summary_text += f"| {i+1} | {tx_hash[:8]}... | {tx_index} | {ada:.6f} |\n"
            
            summary_text += "\nTo view details for a specific UTXO, use lazy_load=True with utxo_hash parameter.\n"
            
            return [types.TextContent(type="text", text=summary_text)]
        
        # For regular pagination without lazy loading
        effective_page_size = get_config_value("page_size", page_size)
        paginator = PaginatedResponse(utxos_data, effective_page_size)
        page_data = paginator.get_page(page)
        
        # Generate browser text
        browser_text = f"# UTXO Browser for {address}\n\n"
        browser_text += f"Total UTXOs: {len(utxos_data)}\n"
        
        if page_data['total_pages'] > 1:
            browser_text += f"Showing page {page_data['page']} of {page_data['total_pages']}\n"
        
        # Calculate total ADA and assets
        total_ada = 0
        all_assets = {}
        
        for utxo in utxos_data:
            if 'amount' in utxo:
                for amount_entry in utxo['amount']:
                    if amount_entry['unit'] == 'lovelace':
                        total_ada += int(amount_entry['quantity'])
                    else:
                        asset_unit = amount_entry['unit']
                        asset_quantity = int(amount_entry['quantity'])
                        
                        if asset_unit in all_assets:
                            all_assets[asset_unit] += asset_quantity
                        else:
                            all_assets[asset_unit] = asset_quantity
        
        browser_text += f"Total ADA: {lovelace_to_ada(total_ada):.6f}\n"
        browser_text += f"Total Unique Assets: {len(all_assets)}\n\n"
        
        # List UTXOs for this page
        for i, utxo in enumerate(page_data['items']):
            tx_hash = utxo.get('tx_hash', 'Unknown')
            tx_index = utxo.get('output_index', 0)
            
            browser_text += f"## UTXO #{(page_data['page']-1)*page_data['items_per_page'] + i+1}: {tx_hash}#{tx_index}\n"
            
            # Basic information
            browser_text += "### Basic Information\n"
            browser_text += f"- Transaction: {tx_hash}\n"
            browser_text += f"- Output Index: {tx_index}\n"
            browser_text += f"- Block: {utxo.get('block', 'Unknown')}\n"
            
            # Data hash information if present
            data_hash = utxo.get('data_hash')
            if data_hash:
                browser_text += f"- Data Hash: {data_hash}\n"
            
            # Reference script if present
            reference_script = utxo.get('reference_script_hash')
            if reference_script:
                browser_text += f"- Reference Script: {reference_script}\n"
                
                # Try to get script details - cache script data
                script_data_items = await fetch_and_cache_data(
                    endpoint=f"scripts/{reference_script}",
                    cache_table='cache_scripts',
                    key_field='script_hash',
                    key_value=reference_script,
                    ttl_hours=48  # Cache for 48 hours as scripts don't change
                )
                
                if script_data_items and len(script_data_items) > 0:
                    script_data = script_data_items[0]
                    browser_text += f"  - Script Type: {script_data.get('type', 'Unknown')}\n"
                    browser_text += f"  - Script Size: {script_data.get('serialised_size', 'Unknown')} bytes\n"
            
            # Datum information if requested and present
            if include_datum:
                inline_datum = utxo.get('inline_datum')
                if inline_datum:
                    browser_text += f"- Inline Datum: {inline_datum}\n"
            
            # UTXO value
            if 'amount' in utxo:
                ada_amount = 0
                asset_entries = []
                
                for amount_entry in utxo['amount']:
                    if amount_entry['unit'] == 'lovelace':
                        ada_amount = int(amount_entry['quantity'])
                    else:
                        asset_entries.append(amount_entry)
                
                browser_text += f"\n### Value\n"
                browser_text += f"- ADA: {lovelace_to_ada(ada_amount):.6f}\n"
                browser_text += f"- Native Tokens: {len(asset_entries)}\n"
                
                # Include detailed asset information if requested
                if include_assets and asset_entries:
                    # If too many assets, provide summary
                    if len(asset_entries) > 10:
                        browser_text += f"\nThis UTXO contains {len(asset_entries)} assets. "
                        browser_text += f"Use lazy_load=True and utxo_hash={tx_hash} for full details.\n"
                    else:
                        browser_text += "\n#### Asset Details\n"
                        browser_text += "| Asset | Policy ID | Quantity |\n"
                        browser_text += "|-------|-----------|----------|\n"
                        
                        for asset in asset_entries:
                            asset_unit = asset['unit']
                            policy_id = asset_unit[:56] if len(asset_unit) > 56 else asset_unit
                            quantity = int(asset['quantity'])
                            
                            # Try to get human-readable asset name
                            display_name = asset_unit
                            
                            asset_details_items = await fetch_and_cache_data(
                                endpoint=f"assets/{asset_unit}",
                                cache_table='cache_assets',
                                key_field='asset',
                                key_value=asset_unit,
                                ttl_hours=24  # Cache for 24 hours
                            )
                            
                            if asset_details_items and len(asset_details_items) > 0:
                                asset_details = asset_details_items[0]
                                if 'onchain_metadata' in asset_details and asset_details['onchain_metadata']:
                                    if 'name' in asset_details['onchain_metadata']:
                                        display_name = asset_details['onchain_metadata']['name']
                                elif 'metadata' in asset_details and asset_details['metadata']:
                                    if 'name' in asset_details['metadata']:
                                        display_name = asset_details['metadata']['name']
                            
                            browser_text += f"| {display_name} | {policy_id[:8]}... | {quantity} |\n"
            
            browser_text += "\n"  # Add spacing between UTXOs
        
        # Add pagination information
        if page_data['total_pages'] > 1:
            browser_text += format_pagination_info(page_data)
            
        # Add lazy loading instructions
        browser_text += "\n## For Detailed Inspection\n"
        browser_text += "For complete details of a specific UTXO, use:\n"
        browser_text += f"lazy_load=True, utxo_hash=\"[TX_HASH]\", output_index=[INDEX]\n"
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        results = []
        
        if should_chunk:
            chunks = chunk_text(browser_text)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=browser_text))
            
        return results
        
    except Exception as e:
        logger.error(f"Error in address_utxo_browser: {e}")
        logger.error(traceback.format_exc())
        return [types.TextContent(
            type="text",
            text=f"An error occurred while browsing UTXOs: {str(e)}"
        )]



@mcp.tool(name="address_transaction_history", description="Get all transactions involving a Cardano address with filtering options.")
async def address_transaction_history(
    address: str,
    limit: int = 20,
    include_details: bool = True,
    tx_hash: str = None,
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    page_size: int = None,
    chunk_output: bool = None,
    skip_problematic: bool = True,  # New parameter
    max_processing_time: int = 60,  # New parameter
    date_from: str = None,         # New parameter - format: YYYY-MM-DD
    date_to: str = None            # New parameter - format: YYYY-MM-DD
) -> List[types.TextContent]:
    """
    Get all transactions involving a Cardano address with filtering options.
    
    Args:
        address: Cardano address to explore
        limit: Maximum number of transactions to retrieve
        include_details: Whether to include detailed transaction information
        tx_hash: Get details for a specific transaction
        page: Page number for pagination
        page_size: Transactions per page (default: from global config)
        chunk_output: Whether to split large output into chunks
        skip_problematic: Skip transactions that cause timeouts or errors
        max_processing_time: Maximum time (seconds) to spend processing
        date_from: Filter transactions from this date (format: YYYY-MM-DD)
        date_to: Filter transactions to this date (format: YYYY-MM-DD)
        
    Returns:
        Transaction history information
    """
    try:
        processing_start_time = time.time()
        
        # Get transaction history for the address with caching
        txs_data = await fetch_and_cache_data(
            endpoint=f"addresses/{address}/transactions",
            cache_table='cache_address_transactions',
            key_field='address',
            key_value=address,
            validator=validate_transaction_data,
            params={"count": limit, "order": "desc"},
            ttl_hours=6,  # Cache for 6 hours
            use_expired_cache=True  # Use expired cache data as fallback
        )
        
        if not txs_data or len(txs_data) == 0:
            return [types.TextContent(
                type="text",
                text=f"No transactions found for address {address}."
            )]
        
        # Handle specific transaction request
        if tx_hash:
            tx_detail_text = f"# Transaction Details: {tx_hash}\n\n"
            
            # Find if this transaction is in our list
            matching_txs = [tx for tx in txs_data if tx.get('tx_hash') == tx_hash]
            
            if not matching_txs:
                return [types.TextContent(
                    type="text",
                    text=f"Transaction {tx_hash} not found in recent history for {address}. "
                         f"It might be older than the {limit} most recent transactions."
                )]
                
            # Get detailed transaction info
            try:
                tx_data_items = await fetch_and_cache_data(
                    endpoint=f"txs/{tx_hash}",
                    cache_table='cache_block_transactions',
                    key_field='tx_hash',
                    key_value=tx_hash,
                    ttl_hours=48,  # Cache for 48 hours as this doesn't change
                    use_expired_cache=True  # Use expired cache data as fallback
                )
                
                if tx_data_items and len(tx_data_items) > 0:
                    tx_data = tx_data_items[0]
                    
                    # Basic transaction information
                    block = tx_data.get('block', 'Unknown')
                    block_height = tx_data.get('block_height', 'Unknown')
                    block_time = "Unknown"
                    if 'block_time' in tx_data:
                        block_time = datetime.fromtimestamp(tx_data['block_time']).strftime('%Y-%m-%d %H:%M:%S UTC')
                    
                    fees = lovelace_to_ada(tx_data.get('fees', 0))
                    deposit = lovelace_to_ada(tx_data.get('deposit', 0))
                    size = tx_data.get('size', 'Unknown')
                    
                    tx_detail_text += "## Basic Information\n"
                    tx_detail_text += f"- Transaction Hash: {tx_hash}\n"
                    tx_detail_text += f"- Block: {block}\n"
                    tx_detail_text += f"- Block Height: {block_height}\n"
                    tx_detail_text += f"- Timestamp: {block_time}\n"
                    tx_detail_text += f"- Fees: {fees} ADA\n"
                    tx_detail_text += f"- Deposit: {deposit} ADA\n"
                    tx_detail_text += f"- Size: {size} bytes\n"
                    
                    # If we've exceeded processing time, return basic info
                    current_time = time.time()
                    if current_time - processing_start_time > max_processing_time:
                        tx_detail_text += "\n⚠️ Processing time limit reached. Detailed information skipped.\n"
                        
                        # Handle chunking if needed
                        should_chunk = get_config_value("enable_chunking", chunk_output)
                        results = []
                        
                        if should_chunk:
                            chunks = chunk_text(tx_detail_text)
                            for chunk in chunks:
                                results.append(types.TextContent(type="text", text=chunk))
                        else:
                            results.append(types.TextContent(type="text", text=tx_detail_text))
                            
                        return results
                    
                    # Get UTXOs to examine inputs and outputs
                    try:
                        tx_utxos_items = await fetch_and_cache_data(
                            endpoint=f"txs/{tx_hash}/utxos",
                            cache_table='cache_address_utxos',
                            key_field='tx_hash',
                            key_value=tx_hash,
                            ttl_hours=48,  # Cache for 48 hours as this doesn't change
                            use_expired_cache=True  # Use expired cache data as fallback
                        )
                        
                        if tx_utxos_items and len(tx_utxos_items) > 0:
                            tx_utxos = tx_utxos_items[0]
                            inputs = tx_utxos.get('inputs', [])
                            outputs = tx_utxos.get('outputs', [])
                            
                            # Filter inputs and outputs related to this address
                            address_inputs = [input_entry for input_entry in inputs if input_entry.get('address') == address]
                            address_outputs = [output for output in outputs if output.get('address') == address]
                            
                            tx_detail_text += "\n## Transaction Type\n"
                            is_sender = bool(address_inputs)
                            is_receiver = bool(address_outputs)
                            
                            if is_sender and is_receiver:
                                tx_detail_text += "- **Self/Mixed Transaction**: This address both sent and received assets in this transaction\n"
                            elif is_sender:
                                tx_detail_text += "- **Outgoing Transaction**: This address sent assets in this transaction\n"
                            elif is_receiver:
                                tx_detail_text += "- **Incoming Transaction**: This address received assets in this transaction\n"
                            
                            # Asset flow analysis
                            tx_detail_text += "\n## Asset Flow Analysis\n"
                            
                            # Calculate amounts sent and received
                            sent_ada = 0
                            received_ada = 0
                            sent_assets = {}
                            received_assets = {}
                            
                            for input_entry in address_inputs:
                                for amount_entry in input_entry.get('amount', []):
                                    unit = amount_entry.get('unit', '')
                                    quantity = int(amount_entry.get('quantity', 0))
                                    
                                    if unit == 'lovelace':
                                        sent_ada += quantity
                                    else:
                                        sent_assets[unit] = sent_assets.get(unit, 0) + quantity
                            
                            for output in address_outputs:
                                for amount_entry in output.get('amount', []):
                                    unit = amount_entry.get('unit', '')
                                    quantity = int(amount_entry.get('quantity', 0))
                                    
                                    if unit == 'lovelace':
                                        received_ada += quantity
                                    else:
                                        received_assets[unit] = received_assets.get(unit, 0) + quantity
                            
                            # Calculate net flow
                            net_ada = received_ada - sent_ada
                            
                            if sent_ada > 0:
                                tx_detail_text += f"- ADA Sent: {lovelace_to_ada(sent_ada):.6f} ADA\n"
                            if received_ada > 0:
                                tx_detail_text += f"- ADA Received: {lovelace_to_ada(received_ada):.6f} ADA\n"
                            if sent_ada > 0 or received_ada > 0:
                                tx_detail_text += f"- Net ADA Flow: {lovelace_to_ada(net_ada):.6f} ADA\n"
                            
                            # Check if we've exceeded processing time
                            current_time = time.time()
                            if current_time - processing_start_time > max_processing_time:
                                tx_detail_text += "\n⚠️ Processing time limit reached. Asset details skipped.\n"
                            else:
                                # List assets sent
                                if sent_assets:
                                    tx_detail_text += f"\n### Assets Sent\n"
                                    tx_detail_text += "| Asset | Policy ID | Quantity |\n"
                                    tx_detail_text += "|-------|-----------|----------|\n"
                                    
                                    for asset_unit, quantity in sent_assets.items():
                                        policy_id = asset_unit[:56] if len(asset_unit) > 56 else asset_unit
                                        asset_name = asset_unit
                                        
                                        try:
                                            # Get asset name
                                            asset_details_items = await fetch_and_cache_data(
                                                endpoint=f"assets/{asset_unit}",
                                                cache_table='cache_assets',
                                                key_field='asset',
                                                key_value=asset_unit,
                                                ttl_hours=24,  # Cache for 24 hours
                                                timeout=5.0,   # Short timeout
                                                use_expired_cache=True  # Use expired cache data as fallback
                                            )
                                            
                                            if asset_details_items and len(asset_details_items) > 0:
                                                asset_details = asset_details_items[0]
                                                if 'onchain_metadata' in asset_details and asset_details['onchain_metadata']:
                                                    if 'name' in asset_details['onchain_metadata']:
                                                        asset_name = asset_details['onchain_metadata']['name']
                                                elif 'metadata' in asset_details and asset_details['metadata']:
                                                    if 'name' in asset_details['metadata']:
                                                        asset_name = asset_details['metadata']['name']
                                        except Exception as e:
                                            if not skip_problematic:
                                                raise
                                        
                                        tx_detail_text += f"| {asset_name} | {policy_id[:8]}... | {quantity} |\n"
                                
                                # List assets received
                                if received_assets:
                                    tx_detail_text += f"\n### Assets Received\n"
                                    tx_detail_text += "| Asset | Policy ID | Quantity |\n"
                                    tx_detail_text += "|-------|-----------|----------|\n"
                                    
                                    for asset_unit, quantity in received_assets.items():
                                        policy_id = asset_unit[:56] if len(asset_unit) > 56 else asset_unit
                                        asset_name = asset_unit
                                        
                                        try:
                                            # Get asset name
                                            asset_details_items = await fetch_and_cache_data(
                                                endpoint=f"assets/{asset_unit}",
                                                cache_table='cache_assets',
                                                key_field='asset',
                                                key_value=asset_unit,
                                                ttl_hours=24,  # Cache for 24 hours
                                                timeout=5.0,   # Short timeout
                                                use_expired_cache=True  # Use expired cache data as fallback
                                            )
                                            
                                            if asset_details_items and len(asset_details_items) > 0:
                                                asset_details = asset_details_items[0]
                                                if 'onchain_metadata' in asset_details and asset_details['onchain_metadata']:
                                                    if 'name' in asset_details['onchain_metadata']:
                                                        asset_name = asset_details['onchain_metadata']['name']
                                                elif 'metadata' in asset_details and asset_details['metadata']:
                                                    if 'name' in asset_details['metadata']:
                                                        asset_name = asset_details['metadata']['name']
                                        except Exception as e:
                                            if not skip_problematic:
                                                raise
                                        
                                        tx_detail_text += f"| {asset_name} | {policy_id[:8]}... | {quantity} |\n"
                                
                                # List other participants in the transaction
                                current_time = time.time()
                                if current_time - processing_start_time <= max_processing_time:
                                    unique_addresses = set()
                                    for input_entry in inputs:
                                        addr = input_entry.get('address')
                                        if addr and addr != address:
                                            unique_addresses.add(addr)
                                    
                                    for output in outputs:
                                        addr = output.get('address')
                                        if addr and addr != address:
                                            unique_addresses.add(addr)
                                    
                                    if unique_addresses:
                                        tx_detail_text += f"\n## Other Participants\n"
                                        tx_detail_text += f"This transaction involved {len(unique_addresses)} other address(es):\n"
                                        
                                        # Limit to first 5 addresses to avoid excessive output
                                        for i, addr in enumerate(list(unique_addresses)[:5]):
                                            tx_detail_text += f"- {addr}\n"
                                        
                                        if len(unique_addresses) > 5:
                                            tx_detail_text += f"- ... and {len(unique_addresses) - 5} more\n"
                    except Exception as e:
                        if skip_problematic:
                            logger.error(f"Error processing UTXOs for {tx_hash}: {e}")
                            tx_detail_text += "\n⚠️ Error retrieving transaction inputs and outputs.\n"
                        else:
                            raise
                
                    # Check if transaction has metadata
                    try:
                        current_time = time.time()
                        if current_time - processing_start_time <= max_processing_time:
                            metadata_items = await fetch_and_cache_data(
                                endpoint=f"txs/{tx_hash}/metadata",
                                cache_table='cache_metadata_txs_labels',
                                key_field='tx_hash',
                                key_value=tx_hash,
                                ttl_hours=48,  # Cache for 48 hours as this doesn't change
                                use_expired_cache=True  # Use expired cache data as fallback
                            )
                            
                            if metadata_items and len(metadata_items) > 0:
                                tx_detail_text += "\n## Transaction Metadata\n"
                                
                                for meta_entry in metadata_items:
                                    label = meta_entry.get('label', 'Unknown')
                                    json_metadata = meta_entry.get('json_metadata')
                                    
                                    if json_metadata:
                                        tx_detail_text += f"### Label {label}\n"
                                        # Format JSON with indentation for readability
                                        formatted_json = json.dumps(json_metadata, indent=2, ensure_ascii=False)
                                        # Limit metadata size to avoid excessively large outputs
                                        if len(formatted_json) > 500:
                                            tx_detail_text += f"```json\n{formatted_json[:500]}...\n```\n"
                                            tx_detail_text += "(Metadata truncated due to size)\n"
                                        else:
                                            tx_detail_text += f"```json\n{formatted_json}\n```\n"
                    except Exception as e:
                        if skip_problematic:
                            logger.error(f"Error retrieving metadata for {tx_hash}: {e}")
                            tx_detail_text += "\n⚠️ Error retrieving transaction metadata.\n"
                        else:
                            raise
                
                    # Handle chunking if needed
                    should_chunk = get_config_value("enable_chunking", chunk_output)
                    results = []
                    
                    if should_chunk:
                        chunks = chunk_text(tx_detail_text)
                        for chunk in chunks:
                            results.append(types.TextContent(type="text", text=chunk))
                    else:
                        results.append(types.TextContent(type="text", text=tx_detail_text))
                        
                    return results
                else:
                    return [types.TextContent(
                        type="text",
                        text=f"Error retrieving transaction details for {tx_hash}."
                    )]
            except Exception as e:
                if skip_problematic:
                    logger.error(f"Error getting transaction details for {tx_hash}: {e}")
                    return [types.TextContent(
                        type="text",
                        text=f"Error retrieving transaction details for {tx_hash}: {str(e)}\n\nTry using skip_problematic=True to get partial information."
                    )]
                else:
                    raise
                    
        # Process transactions - filter by date if specified
        filtered_txs = []
        date_from_obj = None
        date_to_obj = None
        
        if date_from:
            try:
                date_from_obj = datetime.strptime(date_from, '%Y-%m-%d')
            except ValueError:
                return [types.TextContent(
                    type="text",
                    text=f"Invalid date_from format. Please use YYYY-MM-DD format."
                )]
            
        if date_to:
            try:
                date_to_obj = datetime.strptime(date_to, '%Y-%m-%d')
                # Set to end of day
                date_to_obj = date_to_obj.replace(hour=23, minute=59, second=59)
            except ValueError:
                return [types.TextContent(
                    type="text",
                    text=f"Invalid date_to format. Please use YYYY-MM-DD format."
                )]
        
        for tx_info in txs_data:
            tx_hash = tx_info.get('tx_hash', 'Unknown')
            if tx_hash == 'Unknown':
                continue
                
            # Check processing time limit
            current_time = time.time()
            if current_time - processing_start_time > max_processing_time:
                logger.warning(f"Maximum processing time ({max_processing_time}s) exceeded. Processed {len(filtered_txs)} of {len(txs_data)} transactions.")
                break
                
            try:
                # Get transaction details for date filtering
                if date_from_obj or date_to_obj:
                    tx_data_items = await fetch_and_cache_data(
                        endpoint=f"txs/{tx_hash}",
                        cache_table='cache_block_transactions',
                        key_field='tx_hash',
                        key_value=tx_hash,
                        ttl_hours=48,  # Cache for 48 hours
                        use_expired_cache=True
                    )
                    
                    if tx_data_items and len(tx_data_items) > 0:
                        tx_data = tx_data_items[0]
                        if 'block_time' in tx_data:
                            tx_date = datetime.fromtimestamp(tx_data['block_time'])
                            
                            # Apply date filters
                            if date_from_obj and tx_date < date_from_obj:
                                continue
                            if date_to_obj and tx_date > date_to_obj:
                                continue
                    
                # Add additional info to transaction
                tx_info_extended = tx_info.copy()
                
                # Add transaction to filtered list
                filtered_txs.append(tx_info_extended)
                
            except Exception as e:
                logger.error(f"Error processing transaction {tx_hash}: {e}")
                if not skip_problematic:
                    raise
        
        # If no transactions after filtering
        if not filtered_txs:
            filter_text = ""
            if date_from:
                filter_text += f" from {date_from}"
            if date_to:
                filter_text += f" to {date_to}"
                
            return [types.TextContent(
                type="text",
                text=f"No transactions found for address {address}{filter_text}."
            )]

        # Handle normal pagination for detailed view
        effective_page_size = get_config_value("page_size", page_size)
        paginator = PaginatedResponse(filtered_txs, effective_page_size)
        page_data = paginator.get_page(page)
        
        # Generate paginated history text
        history_text = f"# Transaction History for {address}\n\n"
        history_text += f"Total transactions: {page_data['total_items']}\n"
        
        if date_from or date_to:
            history_text += "## Filter Information\n"
            if date_from:
                history_text += f"- From Date: {date_from}\n"
            if date_to:
                history_text += f"- To Date: {date_to}\n"
            history_text += "\n"
        
        if page_data['total_pages'] > 1:
            history_text += f"Showing page {page_data['page']} of {page_data['total_pages']}\n"
        
        # Summary view of transactions on this page
        history_text += "\n## Transactions Summary\n"
        history_text += "| TX Hash | Block | Time | Direction | Amount (ADA) |\n"
        history_text += "|---------|-------|------|-----------|-------------|\n"
        
        problematic_txs = 0
        for tx_info in page_data['items']:
            tx_hash = tx_info.get('tx_hash', 'Unknown')
            block = tx_info.get('block_height', 'Unknown')
            
            try:
                # Check if we've exceeded processing time
                current_time = time.time()
                if current_time - processing_start_time > max_processing_time:
                    history_text += f"\n⚠️ Processing time limit reached. Processed {len(page_data['items']) - problematic_txs - (len(page_data['items']) - (page_data['items'].index(tx_info)))} of {len(page_data['items'])} transactions on this page.\n"
                    break
                
                # Get detailed transaction info - cache each transaction
                tx_data_items = await fetch_and_cache_data(
                    endpoint=f"txs/{tx_hash}",
                    cache_table='cache_block_transactions',
                    key_field='tx_hash',
                    key_value=tx_hash,
                    ttl_hours=48,  # Cache for 48 hours
                    use_expired_cache=True
                )
                
                timestamp = "Unknown"
                direction = "Unknown"
                ada_amount = 0
                
                if tx_data_items and len(tx_data_items) > 0:
                    tx_data = tx_data_items[0]
                    # Get timestamp
                    if 'block_time' in tx_data:
                        timestamp_utc = datetime.fromtimestamp(tx_data['block_time']).strftime('%Y-%m-%d %H:%M:%S')
                        timestamp = timestamp_utc
                    
                    # Determine direction and amount by examining UTXOs
                    tx_utxos_items = await fetch_and_cache_data(
                        endpoint=f"txs/{tx_hash}/utxos",
                        cache_table='cache_address_utxos',
                        key_field='tx_hash',
                        key_value=tx_hash,
                        ttl_hours=48,  # Cache for 48 hours
                        use_expired_cache=True
                    )
                    
                    if tx_utxos_items and len(tx_utxos_items) > 0:
                        tx_utxos = tx_utxos_items[0]
                        inputs = tx_utxos.get('inputs', [])
                        outputs = tx_utxos.get('outputs', [])
                        
                        # Check if address is in inputs (sending)
                        is_sender = any(input_entry.get('address') == address for input_entry in inputs)
                        
                        # Check if address is in outputs (receiving)
                        is_receiver = any(output_entry.get('address') == address for output_entry in outputs)
                        
                        if is_sender and is_receiver:
                            direction = "Self/Mixed"
                        elif is_sender:
                            direction = "Outgoing"
                        elif is_receiver:
                            direction = "Incoming"
                        
                        # Calculate ADA amount
                        sent_ada = 0
                        received_ada = 0
                        
                        for input_entry in inputs:
                            if input_entry.get('address') == address:
                                for amount_entry in input_entry.get('amount', []):
                                    if amount_entry.get('unit') == 'lovelace':
                                        sent_ada += int(amount_entry.get('quantity', 0))
                        
                        for output in outputs:
                            if output.get('address') == address:
                                for amount_entry in output.get('amount', []):
                                    if amount_entry.get('unit') == 'lovelace':
                                        received_ada += int(amount_entry.get('quantity', 0))
                        
                        if direction == "Incoming":
                            ada_amount = received_ada
                        elif direction == "Outgoing":
                            ada_amount = sent_ada - received_ada
                        else:  # Self/Mixed
                            ada_amount = received_ada - sent_ada
                
                # Display transaction summary
                history_text += f"| {tx_hash[:8]}... | {block} | {timestamp} | {direction} | {lovelace_to_ada(ada_amount):.6f} |\n"
                
                # Store additional info for detailed section
                tx_info['timestamp'] = timestamp
                tx_info['direction'] = direction
                tx_info['ada_amount'] = ada_amount
                
            except Exception as e:
                logger.error(f"Error processing transaction {tx_hash}: {e}")
                problematic_txs += 1
                
                if skip_problematic:
                    history_text += f"| {tx_hash[:8]}... | {block} | Error | Error | Error |\n"
                else:
                    raise
        
        # Include detailed transaction information if requested
        if include_details:
            history_text += "\n## Transaction Details\n"
            
            for tx_info in page_data['items']:
                tx_hash = tx_info.get('tx_hash', 'Unknown')
                
                # Skip if this transaction was problematic in summary section
                if 'timestamp' not in tx_info and skip_problematic:
                    continue
                    
                # Check if we've exceeded processing time
                current_time = time.time()
                if current_time - processing_start_time > max_processing_time:
                    history_text += f"\n⚠️ Processing time limit reached. Not all transaction details are displayed.\n"
                    break
                
                # Skip full details in paginated view to keep output manageable
                # Just show basic information
                history_text += f"\n### Transaction {tx_hash}\n"
                
                # Get values from summary section if available
                timestamp = tx_info.get('timestamp', 'Unknown')
                direction = tx_info.get('direction', 'Unknown')
                ada_amount = tx_info.get('ada_amount', 0)
                
                history_text += f"- Time: {timestamp}\n"
                history_text += f"- Direction: {direction}\n"
                history_text += f"- ADA Amount: {lovelace_to_ada(ada_amount):.6f}\n"
                
                # Add metadata summary if possible
                try:
                    metadata_items = await fetch_and_cache_data(
                        endpoint=f"txs/{tx_hash}/metadata",
                        cache_table='cache_metadata_txs_labels',
                        key_field='tx_hash',
                        key_value=tx_hash,
                        ttl_hours=48,  # Cache for 48 hours
                        use_expired_cache=True
                    )
                    
                    if metadata_items and len(metadata_items) > 0:
                        meta_labels = [item.get('label', 'Unknown') for item in metadata_items]
                        history_text += f"- Metadata Labels: {', '.join(meta_labels)}\n"
                except Exception as e:
                    if not skip_problematic:
                        raise
                
                # Add a link to get full transaction details
                history_text += f"\nFor complete details, use: tx_hash=\"{tx_hash}\"\n"
        
        # Add pagination information
        if page_data['total_pages'] > 1:
            history_text += format_pagination_info(page_data)
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        results = []
        
        if should_chunk:
            chunks = chunk_text(history_text)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=history_text))
            
        return results
        
    except Exception as e:
        logger.error(f"Error in address_transaction_history: {e}")
        logger.error(traceback.format_exc())
        
        error_text = f"# Error Retrieving Transaction History\n\n"
        error_text += f"An error occurred while retrieving transaction history: {str(e)}\n\n"
        error_text += "To work around this issue, you can try:\n"
        error_text += "1. Reducing the limit parameter to retrieve fewer transactions\n"
        error_text += "2. Using pagination to get smaller chunks of data\n"
        error_text += "3. Setting skip_problematic=True to handle errors gracefully\n"
        error_text += "4. Increasing max_processing_time if needed\n\n"
        
        return [types.TextContent(type="text", text=error_text)]

# token_holders_analysis function
@mcp.tool(name="token_holders_analysis", description="Get top holders of a specific Cardano token with distribution analysis.")
async def token_holders_analysis(
    token_identifier: str, 
    holders_count: int = 20, 
    include_distribution: bool = True,
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    page_size: int = None,
    summary_only: bool = False,
    chunk_output: bool = None
) -> List[types.TextContent]:
    """
    Get top holders of a specific Cardano token with distribution analysis.
    
    Args:
        token_identifier: The token identifier (policy ID or full unit)
        holders_count: Number of top holders to display (max 100)
        include_distribution: Whether to include distribution analysis
        page: Page number for pagination
        page_size: Items per page (default: from global config)
        summary_only: Return only a summary without holder details
        chunk_output: Whether to split large output into chunks
    """
    global redis_cache
    try:
        # Use token_identifier directly - no resolution logic
        unit = token_identifier
        logger.info(f"Using token identifier directly: {unit}")
        
        # Try to get cached data first
        holders_data = await redis_cache.get_cached_data(
            table='cache_taptools_token_holders',
            key_field='token_unit',
            key_value=unit,
            limit=100
        )
        
        api_error = None
        # If not in cache, try to fetch from API
        if not holders_data:
            logger.info(f"No cached data found, fetching from TapTools API")
            
            # Calculate how many pages to fetch
            pages_to_fetch = (holders_count + 19) // 20  # Ceiling division
            pages_to_fetch = min(pages_to_fetch, 5)  # Limit to 5 pages (100 holders)
            
            all_holders = []
            try:
                for page_num in range(1, pages_to_fetch + 1):
                    holders_result, error = await taptools_api.get_token_holders(
                        unit=unit,
                        page=page_num,
                        per_page=20
                    )
                    
                    if error:
                        api_error = error
                        logger.warning(f"API error on page {page_num}: {error}")
                        break
                    
                    if holders_result:
                        all_holders.extend(holders_result)
                        
                        # Apply rate limiting between pages
                        if page_num < pages_to_fetch:
                            await asyncio.sleep(0.2)  # 200ms delay between pages
                    
                    # Stop if we have enough holders
                    if len(all_holders) >= holders_count:
                        break
                
                # Cache the results if we got any
                if all_holders:
                    # Add token_unit to each holder for caching
                    for holder in all_holders:
                        holder['token_unit'] = unit
                    
                    redis_cache.store_cached_data(
                        table='cache_taptools_token_holders',
                        key_field='token_unit',
                        key_value=unit,
                        data_items=all_holders,
                        ttl_hours=6
                    )
                    
                    holders_data = all_holders
            except Exception as fetch_error:
                logger.error(f"Error during API fetch: {fetch_error}")
                api_error = str(fetch_error)
        
        # If no data was found, return the token database to help the user
        if not holders_data or len(holders_data) == 0:
            # Get the full token database
            all_tokens = await get_asset_database()
            
            # Format basic response
            analysis_text = "# Token Holders Analysis\n\n"
            
            if api_error:
                analysis_text += f"## Error Retrieving Holders\n"
                analysis_text += f"Unable to retrieve token holders for: {token_identifier}\n"
                analysis_text += f"Error: {api_error}\n\n"
            else:
                analysis_text += f"## No Holders Found\n"
                analysis_text += f"No holder data was found for token: {token_identifier}\n\n"
            
            # Return the entire token database for reference
            analysis_text += "## Token Database\n"
            analysis_text += "Here's the complete token database. Use the policy ID + hex name for exact matching:\n\n"
            
            analysis_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
            analysis_text += "|------|--------|-----------|----------|----------|\n"
            
            # Limit the display to keep response size reasonable
            display_tokens = all_tokens[:100] if len(all_tokens) > 100 else all_tokens
            
            for token in display_tokens:
                name = token.get('registry_name') or token.get('name_small') or 'Unknown'
                ticker = token.get('registry_ticker', '')
                policy = token.get('policy', '')
                name = token.get('name', '')
                full_unit = f"{policy}{name}"
                
                analysis_text += f"| {name} | {ticker} | {policy} | {name} | {full_unit} |\n"
            
            if len(all_tokens) > 100:
                analysis_text += f"\n*Showing 100 of {len(all_tokens)} tokens*\n"
            
            # Add instructions
            analysis_text += "\n## Usage Instructions\n"
            analysis_text += "1. Find your desired token in the table above\n"
            analysis_text += "2. Copy the 'Full Unit' value (policy ID + hex name)\n"
            analysis_text += "3. Retry with that exact value as the token_identifier\n"
            
            return [types.TextContent(type="text", text=analysis_text)]
        
        # Calculate total supply for percentage calculation
        total_supply = sum(holder.get('amount', 0) for holder in holders_data)
        
        # Generate token information
        analysis_text = "# Token Holders Analysis\n\n"
        
        # Calculate concentration metrics for distribution analysis
        top_holder_percent = (holders_data[0].get('amount', 0) / total_supply * 100) if total_supply > 0 and holders_data else 0
        top5_percent = (sum(holder.get('amount', 0) for holder in holders_data[:5]) / total_supply * 100) if total_supply > 0 and len(holders_data) >= 5 else 0
        top10_percent = (sum(holder.get('amount', 0) for holder in holders_data[:10]) / total_supply * 100) if total_supply > 0 and len(holders_data) >= 10 else 0
        top20_percent = (sum(holder.get('amount', 0) for holder in holders_data[:20]) / total_supply * 100) if total_supply > 0 and len(holders_data) >= 20 else 0
        
        # Add basic token info
        analysis_text += f"## Token: {unit}\n"
        analysis_text += f"- Total Holders Analyzed: {len(holders_data)}\n"
        
        # If summary_only is True, return just distribution metrics
        if summary_only:
            analysis_text += "## Distribution Summary\n"
            analysis_text += f"- Top holder owns: {top_holder_percent:.2f}% of supply\n"
            analysis_text += f"- Top 5 holders own: {top5_percent:.2f}% of supply\n"
            analysis_text += f"- Top 10 holders own: {top10_percent:.2f}% of supply\n"
            analysis_text += f"- Top 20 holders own: {top20_percent:.2f}% of supply\n\n"
            
            # Categorize distribution
            if top10_percent > 90:
                analysis_text += "Distribution assessment: **Highly concentrated**. A small number of addresses control the vast majority of tokens.\n"
            elif top10_percent > 70:
                analysis_text += "Distribution assessment: **Concentrated**. The token distribution shows significant concentration among top holders.\n"
            elif top10_percent > 50:
                analysis_text += "Distribution assessment: **Moderately distributed**. While top holders have significant stakes, the token shows some distribution.\n"
            else:
                analysis_text += "Distribution assessment: **Well distributed**. The token is relatively evenly distributed among many holders.\n"
            
            return [types.TextContent(type="text", text=analysis_text)]
        
        # Apply pagination for holder list
        effective_page_size = get_config_value("page_size", page_size)
        paginator = PaginatedResponse(holders_data, effective_page_size)
        page_data = paginator.get_page(page)
        
        # Update analysis text with pagination info if needed
        if page_data['total_pages'] > 1:
            analysis_text += f"Showing page {page_data['page']} of {page_data['total_pages']} "
            analysis_text += f"(total holders: {page_data['total_items']})\n\n"
        
        # Add holder table
        analysis_text += "## Top Holders\n"
        analysis_text += "| Rank | Address | Amount | Percentage |\n"
        analysis_text += "|------|---------|--------|------------|\n"
        
        # Display holders for this page
        for i, holder in enumerate(page_data['items']):
            rank = (page_data['page'] - 1) * page_data['items_per_page'] + i + 1
            address = holder.get('address', 'Unknown')
            amount = holder.get('amount', 0)
            percentage = (amount / total_supply * 100) if total_supply > 0 else 0
            
            analysis_text += f"| {rank} | {address} | {amount:,.2f} | {percentage:.2f}% |\n"
        
        # Add distribution analysis if requested
        if include_distribution:
            analysis_text += "\n## Distribution Analysis\n"
            
            analysis_text += "### Concentration Metrics\n"
            analysis_text += f"- Top holder owns: {top_holder_percent:.2f}% of supply\n"
            analysis_text += f"- Top 5 holders own: {top5_percent:.2f}% of supply\n"
            analysis_text += f"- Top 10 holders own: {top10_percent:.2f}% of supply\n"
            analysis_text += f"- Top 20 holders own: {top20_percent:.2f}% of supply\n\n"
            
            # Categorize distribution
            if top10_percent > 90:
                analysis_text += "Distribution assessment: **Highly concentrated**. A small number of addresses control the vast majority of tokens.\n"
            elif top10_percent > 70:
                analysis_text += "Distribution assessment: **Concentrated**. The token distribution shows significant concentration among top holders.\n"
            elif top10_percent > 50:
                analysis_text += "Distribution assessment: **Moderately distributed**. While top holders have significant stakes, the token shows some distribution.\n"
            else:
                analysis_text += "Distribution assessment: **Well distributed**. The token is relatively evenly distributed among many holders.\n"
            
            # Add Lorenz curve data points
            analysis_text += "\n### Distribution Curve\n"
            analysis_text += "| Holder Percentile | Supply Owned |\n"
            analysis_text += "|-------------------|-------------|\n"
            
            # Calculate percentiles (10%, 20%, etc.)
            percentiles = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
            
            for percentile in percentiles:
                holder_count = len(holders_data)
                cutoff_index = min(int(holder_count * percentile / 100), holder_count - 1)
                amount_sum = sum(holder.get('amount', 0) for holder in holders_data[:cutoff_index + 1])
                percent_owned = (amount_sum / total_supply * 100) if total_supply > 0 else 0
                
                analysis_text += f"| Top {percentile}% | {percent_owned:.2f}% |\n"
        
        # Add pagination information
        if page_data['total_pages'] > 1:
            analysis_text += format_pagination_info(page_data)
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        results = []
        
        if should_chunk:
            chunks = chunk_text(analysis_text)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=analysis_text))
        
        return results
        
    except Exception as e:
        logger.error(f"Error in token_holders_analysis: {e}")
        logger.error(traceback.format_exc())
        
        error_text = f"# Error Retrieving Token Holder Data\n\n"
        error_text += f"An error occurred while retrieving token holder data: {str(e)}\n"
        
        # Get the full token database to help user
        all_tokens = await get_asset_database()
        
        if all_tokens:
            error_text += "\n## Available Tokens\n"
            error_text += "Here are some popular tokens you can try:\n\n"
            
            error_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
            error_text += "|------|--------|-----------|----------|----------|\n"
            
            # Show top 15 tokens by quantity
            sorted_tokens = sorted(all_tokens, key=lambda x: x.get('quantity', 0) or 0, reverse=True)
            for token in sorted_tokens[:15]:
                name = token.get('registry_name') or token.get('name_small') or 'Unknown'
                ticker = token.get('registry_ticker', '')
                policy = token.get('policy', '')
                name = token.get('name', '')
                full_unit = f"{policy}{name}"
                
                error_text += f"| {name} | {ticker} | {policy} | {name} | {full_unit} |\n"
        
        return [types.TextContent(type="text", text=error_text)]
        

# Add this main tool at the end of your file
# Fixed loan_offers_analysis function
# loan_offers_analysis function
@mcp.tool(name="loan_offers_analysis", description="Analyze P2P loan offers for a specific Cardano token.")
async def loan_offers_analysis(
    token_identifier: str,
    include: str = "collateral,debt",
    sort_by: str = "time",
    order: str = "desc",
    offers_count: int = 50,
    include_analytics: bool = True,
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    page_size: int = None,
    summary_only: bool = False,
    chunk_output: bool = None
) -> List[types.TextContent]:
    """
    Analyze P2P loan offers for a specific Cardano token.
    
    Args:
        token_identifier: Token identifier (policy ID or full unit)
        include: Filter to offers where token is used as 'collateral', 'debt', 'interest' or a mix
        sort_by: What to sort results by ('time' or 'duration')
        order: Sort direction ('asc' or 'desc')
        offers_count: Number of loan offers to analyze (max 100)
        include_analytics: Whether to include detailed financial analytics
        page: Page number for pagination
        page_size: Items per page (default: from global config)
        summary_only: Return only a summary without offer details
        chunk_output: Whether to split large output into chunks
    """
    global redis_cache
    try:
        # Use token_identifier directly - no resolution logic
        unit = token_identifier
        logger.info(f"Using token identifier directly: {unit}")
        logger.info(f"Include filter: {include}, Sort by: {sort_by}, Order: {order}")
        
        # Try to get cached data first
        offers_data = await redis_cache.get_cached_data(
            table='cache_taptools_loan_offers',
            key_field='token_unit',
            key_value=unit,
            limit=100
        )
        
        api_error = None
        # If not in cache, try to fetch from API
        if not offers_data:
            logger.info(f"No cached loan offers found, fetching from TapTools API")
            
            # Calculate how many pages to fetch
            pages_to_fetch = (offers_count + 99) // 100  # Ceiling division
            pages_to_fetch = min(pages_to_fetch, 1)  # Start with just one page
            
            all_offers = []
            try:
                for page_num in range(1, pages_to_fetch + 1):
                    # Log the full request details for debugging
                    full_url = f"{TAPTOOLS_BASE_URL}/token/debt/offers?unit={unit}&include={include}&sortBy={sort_by}&order={order}&page={page_num}&perPage=100"
                    logger.info(f"Making request to: {full_url}")
                    logger.info(f"With headers: {taptools_api.headers}")
                    
                    offers_result, error = await taptools_api.get_loan_offers(
                        unit=unit,
                        include=include,
                        sort_by=sort_by,
                        order=order,
                        page=page_num,
                        per_page=100
                    )
                    
                    if error:
                        api_error = error
                        logger.warning(f"API error on page {page_num}: {error}")
                        break
                    
                    if offers_result:
                        all_offers.extend(offers_result)
                        
                        # Apply rate limiting between pages
                        if page_num < pages_to_fetch:
                            await asyncio.sleep(0.2)  # 200ms delay between pages
                    
                    # Stop if we have enough offers
                    if len(all_offers) >= offers_count:
                        break
                
                # Cache the results if we got any
                if all_offers:
                    # Add token_unit to each offer for caching
                    for offer in all_offers:
                        offer['token_unit'] = unit
                    
                    redis_cache.store_cached_data(
                        table='cache_taptools_loan_offers',
                        key_field='token_unit',
                        key_value=unit,
                        data_items=all_offers,
                        ttl_hours=6
                    )
                    
                    offers_data = all_offers
            except Exception as fetch_error:
                logger.error(f"Error during API fetch: {fetch_error}")
                api_error = str(fetch_error)
        
        # If no data was found, return the token database to help the user
        if not offers_data or len(offers_data) == 0:
            # Get the full token database
            all_tokens = await get_asset_database()
            
            # Create response text
            analysis_text = "# Loan Offers Analysis\n\n"
            
            if api_error:
                analysis_text += f"## Error Retrieving Loan Offers\n"
                analysis_text += f"Unable to retrieve loan offers from TapTools API.\n"
                analysis_text += f"Error: {api_error}\n\n"
                analysis_text += f"This could be due to:\n"
                analysis_text += f"- The token may not have any active loan offers\n"
                analysis_text += f"- The API endpoint may be temporarily unavailable\n"
                analysis_text += f"- The token identifier may be incorrect\n\n"
            else:
                analysis_text += f"## No Loan Offers Found\n"
                analysis_text += f"No loan offers were found for token: {token_identifier}\n\n"
            
            # Return the entire token database for reference
            analysis_text += "## Token Database\n"
            analysis_text += "Here's the complete token database. Use the policy ID + hex name for exact matching:\n\n"
            
            analysis_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
            analysis_text += "|------|--------|-----------|----------|----------|\n"
            
            # Limit the display to keep response size reasonable
            display_tokens = all_tokens[:100] if len(all_tokens) > 100 else all_tokens
            
            for token in display_tokens:
                name = token.get('registry_name') or token.get('name_small') or 'Unknown'
                ticker = token.get('registry_ticker', '')
                policy = token.get('policy', '')
                name = token.get('name', '')
                full_unit = f"{policy}{name}"
                
                analysis_text += f"| {name} | {ticker} | {policy} | {name} | {full_unit} |\n"
            
            if len(all_tokens) > 100:
                analysis_text += f"\n*Showing 100 of {len(all_tokens)} tokens*\n"
            
            # Add instructions
            analysis_text += "\n## Usage Instructions\n"
            analysis_text += "1. Find your desired token in the table above\n"
            analysis_text += "2. Copy the 'Full Unit' value (policy ID + hex name)\n"
            analysis_text += "3. Retry with that exact value as the token_identifier\n"
            
            return [types.TextContent(type="text", text=analysis_text)]
        
        # Generate analysis text
        analysis_text = "# Loan Offers Analysis\n\n"
        
        # Add token identifier
        analysis_text += f"## Token: {unit}\n"
        
        # Calculate summary metrics
        protocols = {}
        collateral_tokens = {}
        debt_tokens = {}
        collateral_value_total = 0
        debt_value_total = 0
        durations = []
        health_metrics = []
        interest_rates = []
        
        for offer in offers_data:
            # Track protocols
            protocol = offer.get('protocol', 'Unknown')
            protocols[protocol] = protocols.get(protocol, 0) + 1
            
            # Track collateral tokens
            collateral_token = offer.get('collateralToken', 'Unknown')
            if collateral_token != 'Unknown':
                collateral_tokens[collateral_token] = collateral_tokens.get(collateral_token, 0) + 1
                
            # Track debt tokens
            debt_token = offer.get('debtToken', 'Unknown')
            if debt_token != 'Unknown':
                debt_tokens[debt_token] = debt_tokens.get(debt_token, 0) + 1
                
            # Track values
            collateral_value = float(offer.get('collateralValue', 0))
            debt_value = float(offer.get('debtValue', 0))
            collateral_value_total += collateral_value
            debt_value_total += debt_value
            
            # Track durations
            duration = int(offer.get('duration', 0))
            if duration > 0:
                durations.append(duration)
                
            # Track health
            health = float(offer.get('health', 0))
            if health > 0:
                health_metrics.append(health)
                
            # Calculate and track interest rate if possible
            if debt_value > 0 and 'interestValue' in offer:
                interest_value = float(offer.get('interestValue', 0))
                interest_rate = (interest_value / debt_value) * 100
                duration_days = duration / 86400  # Convert seconds to days
                if duration_days > 0:
                    # Annualized interest rate
                    annual_rate = (interest_rate / duration_days) * 365
                    interest_rates.append(annual_rate)
        
        # Add summary information
        analysis_text += f"- Total Active Offers: {len(offers_data)}\n"
        analysis_text += f"- Total Collateral Value: ${collateral_value_total:,.2f}\n"
        analysis_text += f"- Total Debt Value: ${debt_value_total:,.2f}\n"
        
        if protocols:
            analysis_text += f"\n### Protocols\n"
            for protocol, count in protocols.items():
                analysis_text += f"- {protocol}: {count} offers\n"
        
        # Only if summary_only is True, return just the summary
        if summary_only:
            # Add duration information
            if durations:
                avg_duration = sum(durations) / len(durations)
                min_duration = min(durations)
                max_duration = max(durations)
                analysis_text += f"\n### Loan Durations\n"
                analysis_text += f"- Average: {avg_duration/86400:.1f} days\n"
                analysis_text += f"- Minimum: {min_duration/86400:.1f} days\n"
                analysis_text += f"- Maximum: {max_duration/86400:.1f} days\n"
            
            # Add health metrics
            if health_metrics:
                avg_health = sum(health_metrics) / len(health_metrics)
                min_health = min(health_metrics)
                max_health = max(health_metrics)
                analysis_text += f"\n### Health Factors\n"
                analysis_text += f"- Average: {avg_health:.2f}\n"
                analysis_text += f"- Minimum: {min_health:.2f}\n"
                analysis_text += f"- Maximum: {max_health:.2f}\n"
            
            # Add interest rates
            if interest_rates:
                avg_rate = sum(interest_rates) / len(interest_rates)
                min_rate = min(interest_rates)
                max_rate = max(interest_rates)
                analysis_text += f"\n### Interest Rates (Annual)\n"
                analysis_text += f"- Average: {avg_rate:.2f}%\n"
                analysis_text += f"- Minimum: {min_rate:.2f}%\n"
                analysis_text += f"- Maximum: {max_rate:.2f}%\n"
            
            return [types.TextContent(type="text", text=analysis_text)]
        
        # Apply pagination for offers list
        effective_page_size = get_config_value("page_size", page_size)
        paginator = PaginatedResponse(offers_data, effective_page_size)
        page_data = paginator.get_page(page)
        
        # Update analysis text with pagination info if needed
        if page_data['total_pages'] > 1:
            analysis_text += f"\nShowing page {page_data['page']} of {page_data['total_pages']} "
            analysis_text += f"(total offers: {page_data['total_items']})\n\n"
        
        # Add offers table
        analysis_text += "\n## Loan Offers\n"
        analysis_text += "| Protocol | Collateral | Debt | Health | Duration | Interest |\n"
        analysis_text += "|----------|------------|------|--------|----------|----------|\n"
        
        # Display offers for this page
        for offer in page_data['items']:
            protocol = offer.get('protocol', 'Unknown')
            
            # Format collateral
            collateral_token = offer.get('collateralToken', 'Unknown')
            collateral_amount = float(offer.get('collateralAmount', 0))
            collateral_value = float(offer.get('collateralValue', 0))
            collateral_display = f"{collateral_amount:,.2f} (${collateral_value:,.2f})"
            
            # Format debt
            debt_token = offer.get('debtToken', 'Unknown')
            debt_amount = float(offer.get('debtAmount', 0))
            debt_value = float(offer.get('debtValue', 0))
            debt_display = f"{debt_amount:,.2f} (${debt_value:,.2f})"
            
            # Format health
            health = float(offer.get('health', 0))
            health_display = f"{health:.2f}"
            
            # Format duration
            duration = int(offer.get('duration', 0))
            days = duration / 86400  # Convert seconds to days
            duration_display = f"{days:.1f} days"
            
            # Format interest
            interest_amount = float(offer.get('interestAmount', 0))
            interest_value = float(offer.get('interestValue', 0))
            interest_display = f"{interest_amount:,.2f} (${interest_value:,.2f})"
            
            # Add row to table
            analysis_text += f"| {protocol} | {collateral_display} | {debt_display} | {health_display} | {duration_display} | {interest_display} |\n"
        
        # Include detailed analytics if requested
        if include_analytics:
            analysis_text += "\n## Financial Analysis\n"
            
            # Loan duration analysis
            if durations:
                avg_duration = sum(durations) / len(durations)
                min_duration = min(durations)
                max_duration = max(durations)
                
                analysis_text += "\n### Loan Duration Analysis\n"
                analysis_text += f"- Average Duration: {avg_duration/86400:.1f} days\n"
                analysis_text += f"- Minimum Duration: {min_duration/86400:.1f} days\n"
                analysis_text += f"- Maximum Duration: {max_duration/86400:.1f} days\n"
                
                # Group durations into buckets for distribution
                duration_buckets = {
                    "< 7 days": 0,
                    "7-14 days": 0,
                    "14-30 days": 0,
                    "30-60 days": 0,
                    "60-90 days": 0,
                    "> 90 days": 0
                }
                
                for duration in durations:
                    days = duration / 86400
                    if days < 7:
                        duration_buckets["< 7 days"] += 1
                    elif days < 14:
                        duration_buckets["7-14 days"] += 1
                    elif days < 30:
                        duration_buckets["14-30 days"] += 1
                    elif days < 60:
                        duration_buckets["30-60 days"] += 1
                    elif days < 90:
                        duration_buckets["60-90 days"] += 1
                    else:
                        duration_buckets["> 90 days"] += 1
                
                analysis_text += "\n#### Duration Distribution\n"
                analysis_text += "| Duration Range | Count | Percentage |\n"
                analysis_text += "|----------------|-------|------------|\n"
                
                for bucket, count in duration_buckets.items():
                    percentage = (count / len(durations)) * 100
                    analysis_text += f"| {bucket} | {count} | {percentage:.1f}% |\n"
            
            # Health factor analysis
            if health_metrics:
                avg_health = sum(health_metrics) / len(health_metrics)
                min_health = min(health_metrics)
                max_health = max(health_metrics)
                
                analysis_text += "\n### Health Factor Analysis\n"
                analysis_text += f"- Average Health: {avg_health:.2f}\n"
                analysis_text += f"- Minimum Health: {min_health:.2f}\n"
                analysis_text += f"- Maximum Health: {max_health:.2f}\n"
                
                # Categorize health factors
                health_categories = {
                    "High Risk (< 1.1)": 0,
                    "Medium Risk (1.1-1.3)": 0,
                    "Low Risk (1.3-1.5)": 0,
                    "Very Safe (> 1.5)": 0
                }
                
                for health in health_metrics:
                    if health < 1.1:
                        health_categories["High Risk (< 1.1)"] += 1
                    elif health < 1.3:
                        health_categories["Medium Risk (1.1-1.3)"] += 1
                    elif health < 1.5:
                        health_categories["Low Risk (1.3-1.5)"] += 1
                    else:
                        health_categories["Very Safe (> 1.5)"] += 1
                
                analysis_text += "\n#### Health Factor Distribution\n"
                analysis_text += "| Risk Category | Count | Percentage |\n"
                analysis_text += "|---------------|-------|------------|\n"
                
                for category, count in health_categories.items():
                    percentage = (count / len(health_metrics)) * 100
                    analysis_text += f"| {category} | {count} | {percentage:.1f}% |\n"
            
            # Interest rate analysis
            if interest_rates:
                avg_rate = sum(interest_rates) / len(interest_rates)
                min_rate = min(interest_rates)
                max_rate = max(interest_rates)
                
                analysis_text += "\n### Interest Rate Analysis\n"
                analysis_text += f"- Average Annual Rate: {avg_rate:.2f}%\n"
                analysis_text += f"- Minimum Annual Rate: {min_rate:.2f}%\n"
                analysis_text += f"- Maximum Annual Rate: {max_rate:.2f}%\n"
                
                # Group interest rates into buckets
                rate_buckets = {
                    "< 5%": 0,
                    "5-10%": 0,
                    "10-15%": 0,
                    "15-20%": 0,
                    "20-30%": 0,
                    "> 30%": 0
                }
                
                for rate in interest_rates:
                    if rate < 5:
                        rate_buckets["< 5%"] += 1
                    elif rate < 10:
                        rate_buckets["5-10%"] += 1
                    elif rate < 15:
                        rate_buckets["10-15%"] += 1
                    elif rate < 20:
                        rate_buckets["15-20%"] += 1
                    elif rate < 30:
                        rate_buckets["20-30%"] += 1
                    else:
                        rate_buckets["> 30%"] += 1
                
                analysis_text += "\n#### Interest Rate Distribution\n"
                analysis_text += "| Rate Range | Count | Percentage |\n"
                analysis_text += "|------------|-------|------------|\n"
                
                for bucket, count in rate_buckets.items():
                    percentage = (count / len(interest_rates)) * 100
                    analysis_text += f"| {bucket} | {count} | {percentage:.1f}% |\n"
            
            # Market comparison/analysis
            analysis_text += "\n### Market Insight\n"
            
            # Analyze compared to market trends (placeholder for now)
            analysis_text += "Based on the current offers, "
            
            if interest_rates:
                avg_rate = sum(interest_rates) / len(interest_rates)
                if avg_rate < 8:
                    analysis_text += f"the average interest rate of {avg_rate:.2f}% is relatively low "
                    analysis_text += f"compared to typical DeFi lending rates, suggesting lower risk perception for this asset."
                elif avg_rate < 15:
                    analysis_text += f"the average interest rate of {avg_rate:.2f}% is in line with "
                    analysis_text += f"typical DeFi lending rates, suggesting moderate risk perception."
                else:
                    analysis_text += f"the average interest rate of {avg_rate:.2f}% is relatively high, "
                    analysis_text += f"suggesting lenders perceive higher risk for this asset."
            
            if health_metrics:
                avg_health = sum(health_metrics) / len(health_metrics)
                analysis_text += f"\n\nThe average health factor of {avg_health:.2f} "
                
                if avg_health < 1.2:
                    analysis_text += "indicates that loans are being offered with relatively tight margins, "
                    analysis_text += "which could be concerning in volatile market conditions."
                elif avg_health < 1.5:
                    analysis_text += "suggests moderate collateralization requirements, balancing risk and capital efficiency."
                else:
                    analysis_text += "shows conservative collateralization requirements, prioritizing safety over capital efficiency."
        
        # Add disclaimer
        analysis_text += "\n\n## Disclaimer\n"
        analysis_text += "This analysis is for informational purposes only and not financial advice. "
        analysis_text += "Cryptocurrency lending involves significant risks including potential loss of principal. "
        analysis_text += "Always do your own research before participating in any DeFi protocol."
        
        # Add pagination information
        if page_data['total_pages'] > 1:
            analysis_text += format_pagination_info(page_data)
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        results = []
        
        if should_chunk:
            chunks = chunk_text(analysis_text)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=analysis_text))
        
        return results
        
    except Exception as e:
        logger.error(f"Error in loan_offers_analysis: {e}")
        logger.error(traceback.format_exc())
        
        error_text = "# Error Retrieving Loan Offers Data\n\n"
        error_text += f"An error occurred while retrieving loan offers data: {str(e)}\n"
        
        # Get the full token database to help user
        all_tokens = await get_asset_database()
        
        if all_tokens:
            error_text += "\n## Available Tokens\n"
            error_text += "Here are some popular tokens you can try:\n\n"
            
            error_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
            error_text += "|------|--------|-----------|----------|----------|\n"
            
            # Show top 15 tokens by quantity
            sorted_tokens = sorted(all_tokens, key=lambda x: x.get('quantity', 0) or 0, reverse=True)
            for token in sorted_tokens[:15]:
                name = token.get('registry_name') or token.get('name_small') or 'Unknown'
                ticker = token.get('registry_ticker', '')
                policy = token.get('policy', '')
                name = token.get('name', '')
                full_unit = f"{policy}{name}"
                
                error_text += f"| {name} | {ticker} | {policy} | {name} | {full_unit} |\n"
        
        return [types.TextContent(type="text", text=error_text)]


# Token database lookup tool
@mcp.tool(
    name="token_database_lookup",
    description="""Get the complete Cardano token database for reference with policy IDs and hex names.

    Use this tool when users ask about:
    - Finding the correct policy ID for a token
    - Looking up token hex names for API calls
    - Getting a list of available tokens on Cardano
    - Verifying token names and tickers
    - Searching for specific tokens in the database

    IMPORTANT: Always use this tool first before making token-related API calls to get the correct
    policy ID and hex name format required by other tools.

    Example Use Cases:
    - "What is the policy ID for SHEN token?"
    - "Show me all available tokens"
    - "I need the hex name for MIN token"
    - "Find the policy ID for WMT"

    Returns: Complete token database with names, tickers, policy IDs, and hex names.

    Example Output Format:
    | Name | Ticker | Policy ID | Hex Name | Full Unit |
    | SHEN | SHEN | policy123... | 5348454e | policy123...5348454e |
    """
)
async def token_database_lookup() -> List[types.TextContent]:
    """
    Get the complete Cardano token database for reference.
    This allows users to find the correct policy ID and hex name for tokens.
    """
    try:
        # Get the full asset database
        all_tokens = await get_asset_database()
        
        if not all_tokens:
            return [types.TextContent(
                type="text",
                text="Error: Token database not available."
            )]
        
        # Format the database as a simple table
        database_text = "# Cardano Token Database\n\n"
        database_text += f"Total tokens in database: {len(all_tokens)}\n\n"
        database_text += "## Popular Tokens\n"
        database_text += "Here are the most popular tokens. Use the 'Full Unit' (policy ID + hex name) for API calls:\n\n"
        
        database_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
        database_text += "|------|--------|-----------|----------|----------|\n"
        
        # Sort tokens by quantity but show ALL tokens
        sorted_tokens = sorted(all_tokens, key=lambda x: x.get('quantity', 0) or 0, reverse=True)
        display_tokens = sorted_tokens  # No slicing, show all tokens
        
        for token in display_tokens:
            name = token.get('registry_name') or token.get('name_small') or 'Unknown'
            ticker = token.get('registry_ticker', '')
            policy = token.get('policy', '')
            name = token.get('name', '')
            full_unit = f"{policy}{name}"
            
            database_text += f"| {name} | {ticker} | {policy} | {name} | {full_unit} |\n"
        
        database_text += f"\n*Showing all {len(all_tokens)} tokens by quantity*\n"
        
        # Add usage instructions
        database_text += "\n## Usage Instructions\n"
        database_text += "1. Find your desired token in the table above\n"
        database_text += "2. Copy the 'Full Unit' value\n"
        database_text += "3. Use this value as the token_identifier in token_holders_analysis or loan_offers_analysis\n"
        
        return [types.TextContent(type="text", text=database_text)]
        
    except Exception as e:
        logger.error(f"Error in token_database_lookup: {e}")
        logger.error(traceback.format_exc())
        
        return [types.TextContent(
            type="text",
            text=f"Error retrieving token database: {str(e)}"
        )]

@mcp.tool(name="token_price_indicators", description="Analyze token price indicators with professional Wall Street-grade technical analysis.")
async def token_price_indicators(
    token_identifier: str,
    interval: str = "1d",
    indicator: str = "ma",
    items_count: int = 30,
    include_analysis: bool = True,
    length: int = None,
    smoothing_factor: int = None,
    fast_length: int = None,
    slow_length: int = None,
    signal_length: int = None,
    std_mult: int = None,
    quote: str = "USD",
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    page_size: int = None,
    summary_only: bool = False,
    professional_analysis: bool = True,
    include_market_context: bool = True,
    multi_timeframe_analysis: bool = False,
    detect_patterns: bool = True,
    chunk_output: bool = None
) -> List[types.TextContent]:
    """
    Analyze token price indicators with professional Wall Street-grade technical analysis.
    
    Args:
        token_identifier: Token identifier (policy ID or full unit)
        interval: Time interval (3m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 3d, 1w, 1M)
        indicator: Which indicator to use (ma, ema, rsi, macd, bb, bbw)
        items_count: Number of data points to retrieve (max 1000)
        include_analysis: Whether to include detailed analysis and interpretations
        length: Length of data (used in ma, ema, rsi, bb, bbw)
        smoothing_factor: Smoothing factor for EMA (typically 2)
        fast_length: Length of shorter EMA for MACD
        slow_length: Length of longer EMA for MACD
        signal_length: Length of signal EMA for MACD
        std_mult: Standard deviation multiplier for Bollinger Bands
        quote: Quote currency (e.g., ADA, USD)
        page: Page number for pagination
        page_size: Items per page (default: from global config)
        summary_only: Return only a summary without all data points
        professional_analysis: Include sophisticated Wall Street-level technical analysis
        include_market_context: Include broader market context and correlations
        multi_timeframe_analysis: Analyze multiple timeframes for confirmation
        detect_patterns: Detect common chart patterns in the indicator data
        chunk_output: Whether to split large output into chunks
    """
    global redis_cache
    try:
        # Set default lengths based on indicator if not specified
        if indicator == "ma" and not length:
            length = 14
        elif indicator == "ema" and not length:
            length = 14
        elif indicator == "rsi" and not length:
            length = 14
        elif indicator == "macd":
            if not fast_length:
                fast_length = 12
            if not slow_length:
                slow_length = 26
            if not signal_length:
                signal_length = 9
        elif indicator in ["bb", "bbw"]:
            if not length:
                length = 20
            if not std_mult:
                std_mult = 2
        
        if indicator == "ema" and not smoothing_factor:
            smoothing_factor = 2
            
        # Use token_identifier directly - no resolution logic
        unit = token_identifier
        logger.info(f"Using token identifier directly: {unit}")
        
        # Create cache key including indicator and interval
        cache_key = f"{unit}:{indicator}:{interval}:{quote}"
        
        # Try to get cached data first
        indicators_data = await redis_cache.get_cached_data(
            table='cache_token_indicators',
            key_field='cache_key',
            key_value=cache_key,
            limit=1000
        )
        
        api_error = None
        # If not in cache, try to fetch from API
        if not indicators_data:
            logger.info(f"No cached indicators found, fetching from TapTools API")
            
            indicators_result, error = await taptools_api.get_token_indicators(
                unit=unit,
                interval=interval,
                indicator=indicator,
                items=items_count,
                length=length,
                smoothing_factor=smoothing_factor,
                fast_length=fast_length,
                slow_length=slow_length,
                signal_length=signal_length,
                std_mult=std_mult,
                quote=quote
            )
            
            if error:
                api_error = error
                logger.warning(f"API error: {error}")
            
            if indicators_result:
                # For caching, convert list to list of dicts or process the object structure
                cache_items = []
                if isinstance(indicators_result, list):
                    for i, value in enumerate(indicators_result):
                        cache_items.append({
                            'token_unit':unit,
                            'cache_key': cache_key,
                            'index': i,
                            'indicator_values': value,
                            'indicator': indicator,
                            'interval': interval,
                            'quote': quote
                        })
                elif isinstance(indicators_result, dict):
                    # For complex objects like MACD
                    cache_items.append({
                        'token_unit':unit,
                        'cache_key': cache_key,
                        'index': 0,
                        'indicator_values': indicators_result,
                        'indicator': indicator,
                        'interval': interval,
                        'quote': quote
                    })
                
                # Cache the results if we have any
                if cache_items:
                    
                    redis_cache.store_cached_data(
                        table='cache_token_indicators',
                        key_field='cache_key',
                        key_value=cache_key,
                        data_items=cache_items,
                        validator_function=validate_token_indicators_data,
                        ttl_hours=6
                    )
                    
                indicators_data = cache_items
        
        # If no data was found, return the token database to help the user
        if not indicators_data or len(indicators_data) == 0:
            # Get the full token database
            all_tokens = await get_asset_database()
            
            # Format basic response
            analysis_text = "# Token Price Indicators Analysis\n\n"
            
            if api_error:
                analysis_text += f"## Error Retrieving Indicators\n"
                analysis_text += f"Unable to retrieve price indicators for: {token_identifier}\n"
                analysis_text += f"Error: {api_error}\n\n"
            else:
                analysis_text += f"## No Indicator Data Found\n"
                analysis_text += f"No indicator data was found for token: {token_identifier}\n\n"
                analysis_text += f"Please check that the token is actively traded and has price data available.\n\n"
            
            # Return the entire token database for reference
            analysis_text += "## Token Database\n"
            analysis_text += "Here's a reference of popular tokens. Use the policy ID + hex name for exact matching:\n\n"
            
            analysis_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
            analysis_text += "|------|--------|-----------|----------|----------|\n"
            
            # Limit the display to keep response size reasonable
            display_tokens = all_tokens[:30] if len(all_tokens) > 30 else all_tokens
            
            for token in display_tokens:
                name = token.get('registry_name') or token.get('name_small') or 'Unknown'
                ticker = token.get('registry_ticker', '')
                policy = token.get('policy', '')
                hex_name = token.get('name', '')
                full_unit = f"{policy}{hex_name}"
                
                analysis_text += f"| {name} | {ticker} | {policy[:8]}... | {hex_name} | {full_unit[:16]}... |\n"
            
            if len(all_tokens) > 30:
                analysis_text += f"\n*Showing 30 of {len(all_tokens)} tokens*\n"
            
            # Add instructions
            analysis_text += "\n## Usage Instructions\n"
            analysis_text += "1. Find your desired token in the table above\n"
            analysis_text += "2. Copy the 'Full Unit' value (policy ID + hex name)\n"
            analysis_text += "3. Retry with that exact value as the token_identifier\n"
            analysis_text += "4. Ensure the token is actively traded and has price data\n"
            
            return [types.TextContent(type="text", text=analysis_text)]
        
        # Process indicator data
        # First need to reorganize data from cache format back to original format
        indicator_values = []
        if 'indicator_values' in indicators_data[0]:
            # Complex object like MACD
            indicator_values = indicators_data[0]['indicator_values']
        else:
            # List of values
            sorted_data = sorted(indicators_data, key=lambda x: x.get('index', 0))
            indicator_values = [item.get('value') for item in sorted_data]
        
        # Generate analysis text
        analysis_text = f"# {indicator.upper()} Analysis for {token_identifier}\n\n"
        
        # Add basic info about the request
        analysis_text += "## Parameters\n"
        analysis_text += f"- Indicator: {indicator.upper()}\n"
        analysis_text += f"- Interval: {interval}\n"
        analysis_text += f"- Quote Currency: {quote}\n"
        
        if indicator == "ma" or indicator == "ema":
            analysis_text += f"- Length: {length}\n"
            if indicator == "ema":
                analysis_text += f"- Smoothing Factor: {smoothing_factor}\n"
        elif indicator == "rsi":
            analysis_text += f"- Length: {length}\n"
        elif indicator == "macd":
            analysis_text += f"- Fast Length: {fast_length}\n"
            analysis_text += f"- Slow Length: {slow_length}\n"
            analysis_text += f"- Signal Length: {signal_length}\n"
        elif indicator in ["bb", "bbw"]:
            analysis_text += f"- Length: {length}\n"
            analysis_text += f"- Standard Deviation Multiplier: {std_mult}\n"
        
        # If summary_only is True, return just the latest value and interpretation
        if summary_only:
            analysis_text += "\n## Latest Indicator Value\n"
            
            if isinstance(indicator_values, list) and indicator_values:
                latest_value = indicator_values[0]
                analysis_text += f"- Latest {indicator.upper()}: {latest_value}\n\n"
                
                # Add interpretation based on indicator type
                analysis_text += "## Quick Interpretation\n"
                if indicator == "ma":
                    analysis_text += f"The {length}-period Moving Average is currently at {latest_value}.\n"
                    analysis_text += "A Moving Average smooths price data to create a single flowing line, making it easier to identify the direction of the trend.\n"
                elif indicator == "ema":
                    analysis_text += f"The {length}-period Exponential Moving Average is currently at {latest_value}.\n"
                    analysis_text += "An EMA gives more weight to recent prices, making it more responsive to new information than a simple MA.\n"
                elif indicator == "rsi":
                    analysis_text += f"The {length}-period Relative Strength Index is currently at {latest_value}.\n"
                    
                    if latest_value > 70:
                        analysis_text += "RSI above 70 suggests the asset may be overbought and potentially due for a correction.\n"
                    elif latest_value < 30:
                        analysis_text += "RSI below 30 suggests the asset may be oversold and potentially due for a rebound.\n"
                    else:
                        analysis_text += f"RSI between 30-70 suggests the asset is in a neutral territory.\n"
                elif indicator == "macd":
                    if isinstance(latest_value, dict):
                        macd_line = latest_value.get('macd', 'N/A')
                        signal_line = latest_value.get('signal', 'N/A')
                        histogram = latest_value.get('histogram', 'N/A')
                        
                        analysis_text += f"- MACD Line: {macd_line}\n"
                        analysis_text += f"- Signal Line: {signal_line}\n"
                        analysis_text += f"- Histogram: {histogram}\n\n"
                        
                        if macd_line > signal_line:
                            analysis_text += "MACD line above signal line suggests bullish momentum.\n"
                        else:
                            analysis_text += "MACD line below signal line suggests bearish momentum.\n"
                    else:
                        analysis_text += f"MACD value: {latest_value}\n"
                elif indicator == "bb":
                    if isinstance(latest_value, dict):
                        upper = latest_value.get('upper', 'N/A')
                        middle = latest_value.get('middle', 'N/A')
                        lower = latest_value.get('lower', 'N/A')
                        
                        analysis_text += f"- Upper Band: {upper}\n"
                        analysis_text += f"- Middle Band: {middle}\n"
                        analysis_text += f"- Lower Band: {lower}\n\n"
                        
                        analysis_text += "Bollinger Bands measure volatility. If price is near the upper band, it may be overbought. If price is near the lower band, it may be oversold.\n"
                    else:
                        analysis_text += f"Bollinger Band value: {latest_value}\n"
                elif indicator == "bbw":
                    analysis_text += f"The Bollinger Band Width is currently at {latest_value}.\n"
                    analysis_text += "BBW indicates volatility - higher values suggest increased volatility, while lower values suggest decreased volatility and potential for a breakout.\n"
            else:
                analysis_text += "Unable to retrieve the latest indicator value.\n"
            
            return [types.TextContent(type="text", text=analysis_text)]
        
        # Apply pagination for detailed data points
        effective_page_size = get_config_value("page_size", page_size)
        
        data_points = []
        if isinstance(indicator_values, list):
            data_points = indicator_values
        elif isinstance(indicator_values, dict):
            # For MACD which returns an object, we'll create a list of dictionaries
            macd_data = []
            
            # Assuming MACD structure has arrays for macd, signal, histogram
            macd_values = indicator_values.get('macd', [])
            signal_values = indicator_values.get('signal', [])
            histogram_values = indicator_values.get('histogram', [])
            
            # Combine arrays into a list of dicts for pagination
            for i in range(min(len(macd_values), len(signal_values), len(histogram_values))):
                macd_data.append({
                    'macd': macd_values[i],
                    'signal': signal_values[i],
                    'histogram': histogram_values[i]
                })
            
            data_points = macd_data
        
        paginator = PaginatedResponse(data_points, effective_page_size)
        page_data = paginator.get_page(page)
        
        # Update analysis text with pagination info if needed
        if page_data['total_pages'] > 1:
            analysis_text += f"\nShowing page {page_data['page']} of {page_data['total_pages']} "
            analysis_text += f"(total data points: {page_data['total_items']})\n\n"
        
        # Add data points table based on indicator type
        if indicator in ["ma", "ema"]:
            analysis_text += f"\n## {indicator.upper()} Values\n"
            analysis_text += "| # | Value |\n"
            analysis_text += "|---|-------|\n"
            
            for i, value in enumerate(page_data['items']):
                index = (page_data['page'] - 1) * page_data['items_per_page'] + i + 1
                analysis_text += f"| {index} | {value} |\n"
                
        elif indicator == "rsi":
            analysis_text += "\n## RSI Values\n"
            analysis_text += "| # | Value | Condition |\n"
            analysis_text += "|---|-------|----------|\n"
            
            for i, value in enumerate(page_data['items']):
                index = (page_data['page'] - 1) * page_data['items_per_page'] + i + 1
                condition = "Neutral"
                if value > 70:
                    condition = "Overbought"
                elif value < 30:
                    condition = "Oversold"
                    
                analysis_text += f"| {index} | {value} | {condition} |\n"
                
        elif indicator == "macd":
            analysis_text += "\n## MACD Values\n"
            analysis_text += "| # | MACD | Signal | Histogram | Signal |\n"
            analysis_text += "|---|------|--------|-----------|--------|\n"
            
            for i, value in enumerate(page_data['items']):
                index = (page_data['page'] - 1) * page_data['items_per_page'] + i + 1
                macd = value.get('macd', 'N/A')
                signal = value.get('signal', 'N/A')
                histogram = value.get('histogram', 'N/A')
                
                # Determine bullish/bearish signal
                signal_text = "Neutral"
                if macd > signal:
                    signal_text = "Bullish"
                elif macd < signal:
                    signal_text = "Bearish"
                
                analysis_text += f"| {index} | {macd} | {signal} | {histogram} | {signal_text} |\n"
                
        elif indicator == "bb":
            analysis_text += "\n## Bollinger Bands Values\n"
            analysis_text += "| # | Upper Band | Middle Band | Lower Band |\n"
            analysis_text += "|---|------------|-------------|------------|\n"
            
            for i, value in enumerate(page_data['items']):
                index = (page_data['page'] - 1) * page_data['items_per_page'] + i + 1
                
                if isinstance(value, dict):
                    upper = value.get('upper', 'N/A')
                    middle = value.get('middle', 'N/A')
                    lower = value.get('lower', 'N/A')
                    analysis_text += f"| {index} | {upper} | {middle} | {lower} |\n"
                else:
                    analysis_text += f"| {index} | {value} | N/A | N/A |\n"
                    
        elif indicator == "bbw":
            analysis_text += "\n## Bollinger Band Width Values\n"
            analysis_text += "| # | Width | Volatility |\n"
            analysis_text += "|---|-------|------------|\n"
            
            # Need more context on how to interpret BBW values
            # This is a simple placeholder logic
            for i, value in enumerate(page_data['items']):
                index = (page_data['page'] - 1) * page_data['items_per_page'] + i + 1
                
                volatility = "Normal"
                if value > 0.05:  # Arbitrary threshold
                    volatility = "High"
                elif value < 0.02:  # Arbitrary threshold
                    volatility = "Low"
                
                analysis_text += f"| {index} | {value} | {volatility} |\n"
                
        else:
            # Generic table for any other indicator
            analysis_text += f"\n## {indicator.upper()} Values\n"
            analysis_text += "| # | Value |\n"
            analysis_text += "|---|-------|\n"
            
            for i, value in enumerate(page_data['items']):
                index = (page_data['page'] - 1) * page_data['items_per_page'] + i + 1
                analysis_text += f"| {index} | {value} |\n"
        
        # Include detailed analysis if requested
        if include_analysis:
            analysis_text += "\n## Technical Analysis\n"
            
            if indicator == "ma" or indicator == "ema":
                moving_avg_type = "Moving Average" if indicator == "ma" else "Exponential Moving Average"
                
                analysis_text += f"### {moving_avg_type} Interpretation\n"
                analysis_text += f"The {length}-period {moving_avg_type} is a trend-following or lagging indicator that smooths price data.\n\n"
                
                if len(data_points) >= 2:
                    current_value = data_points[0]
                    prev_value = data_points[1]
                    
                    if current_value > prev_value:
                        analysis_text += f"- **Trend Direction**: The {moving_avg_type} is currently trending upward (rising from {prev_value} to {current_value}).\n"
                        analysis_text += f"- This suggests bullish momentum in the recent {interval} periods.\n"
                    elif current_value < prev_value:
                        analysis_text += f"- **Trend Direction**: The {moving_avg_type} is currently trending downward (falling from {prev_value} to {current_value}).\n"
                        analysis_text += f"- This suggests bearish momentum in the recent {interval} periods.\n"
                    else:
                        analysis_text += f"- **Trend Direction**: The {moving_avg_type} is currently flat (unchanged from {prev_value}).\n"
                        analysis_text += f"- This suggests a consolidation phase or sideways movement.\n"
                
                if professional_analysis:
                    analysis_text += "\n### Professional Trading Analysis\n"
                    analysis_text += f"**Advanced {moving_avg_type} Assessment:**\n\n"
                    
                    # Calculate trend strength
                    if len(data_points) >= 5:
                        values = [point for point in data_points[:5]]
                        avg_change = sum([abs(values[i] - values[i+1]) for i in range(len(values)-1)]) / (len(values)-1)
                        std_dev = (sum([(abs(values[i] - values[i+1]) - avg_change)**2 for i in range(len(values)-1)]) / (len(values)-1))**0.5
                        
                        # Trend strength ratio
                        trend_strength = avg_change / (std_dev if std_dev > 0 else 0.001)
                        
                        if trend_strength > 1.5:
                            analysis_text += f"- **Trend Strength**: Very Strong (Coefficient: {trend_strength:.2f})\n"
                            analysis_text += "- **Institutional Analysis**: A trend strength coefficient above 1.5 typically indicates significant institutional positioning.\n"
                        elif trend_strength > 1.0:
                            analysis_text += f"- **Trend Strength**: Strong (Coefficient: {trend_strength:.2f})\n"
                            analysis_text += "- **Institutional Analysis**: Strong trend coefficients between 1.0-1.5 often reflect sustained institutional interest.\n"
                        elif trend_strength > 0.5:
                            analysis_text += f"- **Trend Strength**: Moderate (Coefficient: {trend_strength:.2f})\n"
                            analysis_text += "- **Institutional Analysis**: Moderate trend strength may indicate early institutional positioning or retail-dominated movement.\n"
                        else:
                            analysis_text += f"- **Trend Strength**: Weak (Coefficient: {trend_strength:.2f})\n"
                            analysis_text += "- **Institutional Analysis**: Weak trend coefficients often indicate choppy conditions with no clear institutional direction.\n"
                    
                    # Add slope analysis
                    if len(data_points) >= 10:
                        short_slope = (data_points[0] - data_points[2]) / 3
                        medium_slope = (data_points[0] - data_points[5]) / 6
                        long_slope = (data_points[0] - data_points[9]) / 10
                        
                        analysis_text += f"\n**Slope Analysis (Rate of Change):**\n"
                        analysis_text += f"- Short-term ({interval} x3): {short_slope:.6f} units/period\n"
                        analysis_text += f"- Medium-term ({interval} x6): {medium_slope:.6f} units/period\n"
                        analysis_text += f"- Long-term ({interval} x10): {long_slope:.6f} units/period\n\n"
                        
                        if short_slope > medium_slope > long_slope and all(x > 0 for x in [short_slope, medium_slope, long_slope]):
                            analysis_text += "**Acceleration Pattern**: Positive acceleration in bullish trend - typically a strong continuation signal.\n"
                        elif short_slope < medium_slope < long_slope and all(x < 0 for x in [short_slope, medium_slope, long_slope]):
                            analysis_text += "**Acceleration Pattern**: Negative acceleration in bearish trend - typically a strong continuation signal.\n"
                        elif short_slope < 0 < medium_slope:
                            analysis_text += "**Inflection Pattern**: Recent trend reversal detected - monitor closely for confirmation.\n"
                    
                    # Add key levels
                    if len(data_points) >= 20:
                        # Create simplified pivot point calculation
                        max_value = max(data_points[:20])
                        min_value = min(data_points[:20])
                        current = data_points[0]
                        pivot = (max_value + min_value + current) / 3
                        r1 = (2 * pivot) - min_value
                        s1 = (2 * pivot) - max_value
                        
                        analysis_text += f"\n**Key Technical Levels:**\n"
                        analysis_text += f"- Major Resistance: {max_value:.6f}\n"
                        analysis_text += f"- Minor Resistance (R1): {r1:.6f}\n"
                        analysis_text += f"- Pivot Point: {pivot:.6f}\n"
                        analysis_text += f"- Minor Support (S1): {s1:.6f}\n"
                        analysis_text += f"- Major Support: {min_value:.6f}\n"
                
                analysis_text += "\n### Trading Strategies\n"
                analysis_text += f"Common trading strategies using the {length}-period {moving_avg_type}:\n\n"
                analysis_text += f"1. **Trend Following**: Trade in the direction of the {moving_avg_type}. Buy when price crosses above the {moving_avg_type}, sell when it crosses below.\n"
                analysis_text += f"2. **Support/Resistance**: Use the {moving_avg_type} as a dynamic support (in uptrends) or resistance (in downtrends) level.\n"
                analysis_text += f"3. **Multiple Timeframes**: Compare {moving_avg_type}s across different timeframes for stronger signals.\n"
                
                if professional_analysis:
                    analysis_text += f"\n### Institutional Trading Strategies\n"
                    analysis_text += f"Advanced strategies employed by professional traders:\n\n"
                    analysis_text += f"1. **{moving_avg_type} Ribbon Trading**: Use a series of {moving_avg_type}s with different lengths (e.g., 10, 20, 50, 100, 200). When shorter MAs cross above longer MAs in sequence ('ribbon expansion'), it signals strong momentum.\n\n"
                    analysis_text += f"2. **Volume-Weighted {moving_avg_type}**: Incorporate volume data to give more weight to high-volume price movements for a more responsive indicator that reduces false signals.\n\n"
                    analysis_text += f"3. **Anchored {moving_avg_type}s**: Calculate {moving_avg_type}s starting from significant market events or pivot points rather than using a fixed lookback period.\n\n"
                    analysis_text += f"4. **Golden Cross / Death Cross**: Long-term trend changes confirmed by 50-period crossing above/below 200-period {moving_avg_type}. These are major institutional signals that often lead to significant capital reallocation.\n"
                
            elif indicator == "rsi":
                analysis_text += "### RSI Interpretation\n"
                analysis_text += f"The {length}-period Relative Strength Index (RSI) is a momentum oscillator that measures the speed and change of price movements.\n"
                analysis_text += "It oscillates between 0 and 100, with traditional interpretation as follows:\n\n"
                analysis_text += "- RSI > 70: Potentially overbought (bearish signal)\n"
                analysis_text += "- RSI < 30: Potentially oversold (bullish signal)\n"
                analysis_text += "- RSI between 30-70: Neutral territory\n\n"
                
                if data_points:
                    current_rsi = data_points[0]
                    
                    if current_rsi > 70:
                        analysis_text += f"**Current Situation**: With RSI at {current_rsi}, the token is currently in **overbought** territory.\n"
                        analysis_text += "This suggests a potential price correction or reversal in the near term. Traders might consider taking profits or preparing for a potential downward movement.\n"
                    elif current_rsi < 30:
                        analysis_text += f"**Current Situation**: With RSI at {current_rsi}, the token is currently in **oversold** territory.\n"
                        analysis_text += "This suggests a potential rebound or reversal in the near term. Traders might consider buying opportunities or preparing for a potential upward movement.\n"
                    else:
                        analysis_text += f"**Current Situation**: With RSI at {current_rsi}, the token is currently in **neutral** territory.\n"
                        analysis_text += "This suggests that the asset is neither overbought nor oversold, and might continue in its current trend without immediate reversal signals.\n"
                    
                    # Add divergence analysis if we have enough data points
                    if len(data_points) >= 5:
                        analysis_text += "\n### RSI Divergence Analysis\n"
                        analysis_text += "Divergence occurs when the price movement and RSI movement don't align, often signaling potential reversals:\n\n"
                        analysis_text += "- **Bullish Divergence**: Price makes lower lows while RSI makes higher lows (potential upward reversal)\n"
                        analysis_text += "- **Bearish Divergence**: Price makes higher highs while RSI makes lower highs (potential downward reversal)\n\n"
                        analysis_text += "To confirm divergences, you should examine the price chart alongside the RSI values.\n"
                    
                    if professional_analysis:
                        analysis_text += "\n### Professional RSI Analysis\n"
                        
                        # Enhanced RSI interpretation with more nuanced levels
                        analysis_text += "**Institutional RSI Framework:**\n\n"
                        analysis_text += "Professional traders often use a more nuanced RSI interpretation:\n"
                        analysis_text += "- 80-100: Extremely overbought - Strong distribution signals\n"
                        analysis_text += "- 70-80: Moderately overbought - Early distribution/profit taking\n"
                        analysis_text += "- 60-70: Strength zone - Bullish but approaching resistance\n"
                        analysis_text += "- 50-60: Mild strength - Typically seen in early bull trends\n"
                        analysis_text += "- 40-50: Mild weakness - Typically seen in early bear trends\n"
                        analysis_text += "- 30-40: Weakness zone - Bearish but approaching support\n"
                        analysis_text += "- 20-30: Moderately oversold - Early accumulation/bottom fishing\n"
                        analysis_text += "- 0-20: Extremely oversold - Strong accumulation signals\n\n"
                        
                        # Identifying zones based on current RSI
                        current_zone = ""
                        zone_description = ""
                        if current_rsi >= 80:
                            current_zone = "Extremely Overbought (80-100)"
                            zone_description = "Strong distribution phase; institutional selling typically dominates this zone. High probability of reversal."
                        elif current_rsi >= 70:
                            current_zone = "Moderately Overbought (70-80)"
                            zone_description = "Early distribution phase; smart money begins taking profits while retail continues buying."
                        elif current_rsi >= 60:
                            current_zone = "Strength Zone (60-70)"
                            zone_description = "Healthy bullish momentum with potential for continuation, though approaching resistance levels."
                        elif current_rsi >= 50:
                            current_zone = "Mild Strength (50-60)"
                            zone_description = "Early bull trend development; institutional accumulation typically occurs in this range."
                        elif current_rsi >= 40:
                            current_zone = "Mild Weakness (40-50)"
                            zone_description = "Early bear trend development; institutional distribution often occurs in this range."
                        elif current_rsi >= 30:
                            current_zone = "Weakness Zone (30-40)"
                            zone_description = "Bearish momentum with potential for continuation, though approaching support levels."
                        elif current_rsi >= 20:
                            current_zone = "Moderately Oversold (20-30)"
                            zone_description = "Early accumulation phase; smart money begins buying while retail continues selling."
                        else:
                            current_zone = "Extremely Oversold (0-20)"
                            zone_description = "Strong accumulation phase; institutional buying typically dominates this zone. High probability of reversal."
                        
                        analysis_text += f"**Current RSI Zone**: {current_zone}\n"
                        analysis_text += f"**Zone Interpretation**: {zone_description}\n\n"
                        
                        # Add pattern recognition if enough data
                        if len(data_points) >= 20:
                            analysis_text += "**RSI Pattern Recognition:**\n"
                            
                            # Check for RSI centerline crossovers (50 level)
                            crossovers = []
                            for i in range(1, len(data_points)):
                                if (data_points[i-1] > 50 and data_points[i] < 50) or (data_points[i-1] < 50 and data_points[i] > 50):
                                    crossovers.append(i)
                            
                            if crossovers:
                                analysis_text += f"- RSI centerline (50) crossovers detected at positions: {', '.join(map(str, crossovers))}\n"
                                analysis_text += "  These centerline crossovers are significant momentum shifts used by institutional traders.\n"
                            
                            # Check for RSI failure swings (high probability reversal signals)
                            failure_swing_high = False
                            failure_swing_low = False
                            
                            # Simplified check for demonstration
                            if len(data_points) >= 6:
                                # Check for bearish failure swing (failure swing high)
                                if data_points[4] > 70 and data_points[2] > data_points[4] and data_points[0] < data_points[2]:
                                    failure_swing_high = True
                                
                                # Check for bullish failure swing (failure swing low)
                                if data_points[4] < 30 and data_points[2] < data_points[4] and data_points[0] > data_points[2]:
                                    failure_swing_low = True
                            
                            if failure_swing_high:
                                analysis_text += "- **Bearish Failure Swing** detected: High-probability reversal signal to the downside.\n"
                                analysis_text += "  This pattern is closely watched by institutional traders as it suggests exhaustion of buying momentum.\n"
                            
                            if failure_swing_low:
                                analysis_text += "- **Bullish Failure Swing** detected: High-probability reversal signal to the upside.\n"
                                analysis_text += "  This pattern is closely watched by institutional traders as it suggests exhaustion of selling momentum.\n"
                            
                            # Check for positive/negative reversals (Cardwell patterns)
                            if len(data_points) >= 10:
                                potential_bullish_reversal = data_points[9] < 30 and data_points[0] > 40 and data_points[0] < 60
                                potential_bearish_reversal = data_points[9] > 70 and data_points[0] < 60 and data_points[0] > 40
                                
                                if potential_bullish_reversal:
                                    analysis_text += "- **Potential Bullish Reversal** pattern: RSI moved from oversold to mid-range, indicating positive momentum shift.\n"
                                
                                if potential_bearish_reversal:
                                    analysis_text += "- **Potential Bearish Reversal** pattern: RSI moved from overbought to mid-range, indicating negative momentum shift.\n"
                        
                        # Add range analysis
                        if len(data_points) >= 10:
                            rsi_max = max(data_points[:10])
                            rsi_min = min(data_points[:10])
                            rsi_range = rsi_max - rsi_min
                            
                            analysis_text += f"\n**RSI Range Analysis (Last 10 Periods)**:\n"
                            analysis_text += f"- Maximum RSI: {rsi_max:.2f}\n"
                            analysis_text += f"- Minimum RSI: {rsi_min:.2f}\n"
                            analysis_text += f"- RSI Range: {rsi_range:.2f}\n"
                            
                            if rsi_range > 40:
                                analysis_text += "- **High Volatility**: Wide RSI range indicates significant momentum shifts. Expect larger price movements.\n"
                            elif rsi_range < 20:
                                analysis_text += "- **Low Volatility**: Narrow RSI range suggests momentum consolidation. Potential for breakout.\n"
                            else:
                                analysis_text += "- **Moderate Volatility**: Normal RSI range indicates balanced momentum conditions.\n"
                
                analysis_text += "\n### Trading Strategies\n"
                analysis_text += "Common RSI trading strategies:\n\n"
                analysis_text += "1. **Overbought/Oversold Reversals**: Buy when RSI moves below 30 then back above it; sell when RSI moves above 70 then back below it.\n"
                analysis_text += "2. **Divergence Trading**: Look for divergences between price action and RSI for potential reversal signals.\n"
                analysis_text += "3. **Trendline Breaks**: Draw trendlines on the RSI chart and trade breaks of these lines.\n"
                analysis_text += "4. **Range-Bound Markets**: Use 70/30 levels to identify potential tops and bottoms in ranging markets.\n"
                
                if professional_analysis:
                    analysis_text += "\n### Professional Trading Techniques\n"
                    analysis_text += "Advanced RSI methods used by institutional traders:\n\n"
                    analysis_text += "1. **RSI with Double Smoothing**: Apply a second EMA to the RSI to reduce noise and identify stronger momentum shifts.\n"
                    analysis_text += "2. **RSI Trend Identification Strategy**: Consider only buy signals when RSI is above 50 and only sell signals when RSI is below 50 to trade with the larger trend.\n"
                    analysis_text += "3. **RSI Bollinger Bands**: Apply Bollinger Bands to the RSI indicator itself to identify extreme readings relative to recent RSI volatility.\n"
                    analysis_text += "4. **Dynamic RSI Thresholds**: Instead of fixed 70/30 levels, calculate historical distribution of RSI values for this specific asset to determine more accurate thresholds.\n"
                    analysis_text += "5. **Wyckoff RSI Method**: Use RSI to identify accumulation/distribution phases, spring/upthrust tests, and signs of effort vs. result.\n"
                
                if detect_patterns and len(data_points) >= 10:
                    # Implement simple pattern detection logic
                    bullish_divergence = False
                    bearish_divergence = False
                    
                    # In a real implementation, you would check actual price data against RSI
                    # This is simplified for demonstration purposes
                    if data_points[0] > data_points[2] and data_points[1] < data_points[3]:
                        bullish_divergence = True
                    elif data_points[0] < data_points[2] and data_points[1] > data_points[3]:
                        bearish_divergence = True
                    
                    if bullish_divergence or bearish_divergence:
                        analysis_text += "\n### Detected Chart Patterns\n"
                        
                        if bullish_divergence:
                            analysis_text += "- **Bullish Divergence**: RSI making higher lows while price makes lower lows.\n"
                            analysis_text += "  This is a high-probability reversal pattern suggesting accumulation and potential upside.\n"
                            analysis_text += "  Target: Previous swing high / Key resistance level.\n"
                            analysis_text += "  Stop Loss: Below the recent swing low.\n"
                        
                        if bearish_divergence:
                            analysis_text += "- **Bearish Divergence**: RSI making lower highs while price makes higher highs.\n"
                            analysis_text += "  This is a high-probability reversal pattern suggesting distribution and potential downside.\n"
                            analysis_text += "  Target: Previous swing low / Key support level.\n"
                            analysis_text += "  Stop Loss: Above the recent swing high.\n"
                
            elif indicator == "macd":
                analysis_text += "### MACD Interpretation\n"
                analysis_text += "The Moving Average Convergence Divergence (MACD) is a trend-following momentum indicator that shows the relationship between two moving averages.\n\n"
                analysis_text += f"Components of MACD (based on your parameters):\n"
                analysis_text += f"- MACD Line: Difference between {fast_length}-period and {slow_length}-period EMAs\n"
                analysis_text += f"- Signal Line: {signal_length}-period EMA of the MACD Line\n"
                analysis_text += "- Histogram: Difference between MACD Line and Signal Line\n\n"
                
                if data_points and isinstance(data_points[0], dict):
                    latest = data_points[0]
                    macd_line = latest.get('macd', 'N/A')
                    signal_line = latest.get('signal', 'N/A')
                    histogram = latest.get('histogram', 'N/A')
                    
                    analysis_text += "**Current MACD Signals**:\n\n"
                    
                    if macd_line > signal_line:
                        analysis_text += "- **Bullish Signal**: MACD line is above the signal line, suggesting bullish momentum.\n"
                        if histogram > 0 and len(data_points) > 1 and histogram > data_points[1].get('histogram', 0):
                            analysis_text += "- Increasing histogram indicates strengthening bullish momentum.\n"
                        elif histogram > 0 and len(data_points) > 1 and histogram < data_points[1].get('histogram', 0):
                            analysis_text += "- Decreasing histogram may indicate weakening bullish momentum, while still positive.\n"
                    else:
                        analysis_text += "- **Bearish Signal**: MACD line is below the signal line, suggesting bearish momentum.\n"
                        if histogram < 0 and len(data_points) > 1 and histogram < data_points[1].get('histogram', 0):
                            analysis_text += "- Decreasing histogram indicates strengthening bearish momentum.\n"
                        elif histogram < 0 and len(data_points) > 1 and histogram > data_points[1].get('histogram', 0):
                            analysis_text += "- Increasing histogram may indicate weakening bearish momentum, while still negative.\n"
                    
                    if macd_line > 0 and signal_line > 0:
                        analysis_text += "- Both MACD and signal lines are positive, suggesting an overall bullish trend.\n"
                    elif macd_line < 0 and signal_line < 0:
                        analysis_text += "- Both MACD and signal lines are negative, suggesting an overall bearish trend.\n"
                    
                analysis_text += "\n### Trading Strategies\n"
                analysis_text += "Common MACD trading strategies:\n\n"
                analysis_text += "1. **Signal Line Crossovers**: Buy when the MACD line crosses above the signal line; sell when it crosses below.\n"
                analysis_text += "2. **Zero Line Crossovers**: Buy when the MACD line crosses above zero; sell when it crosses below zero.\n"
                analysis_text += "3. **Divergence**: Look for divergence between price action and MACD for potential reversal signals.\n"
                analysis_text += "4. **Histogram Analysis**: Monitor changes in the histogram's size to gauge momentum strength.\n"
                
            elif indicator == "bb":
                analysis_text += "### Bollinger Bands Interpretation\n"
                analysis_text += f"Bollinger Bands consist of a middle band (a {length}-period SMA) with an upper and lower band that are {std_mult} standard deviations away from the middle band.\n\n"
                analysis_text += "Bollinger Bands expand during periods of high volatility and contract during periods of low volatility:\n\n"
                analysis_text += "- Price touching or exceeding the upper band suggests potential overbought conditions\n"
                analysis_text += "- Price touching or dropping below the lower band suggests potential oversold conditions\n"
                analysis_text += "- Price moving between bands suggests normal trading range\n\n"
                
                if data_points and isinstance(data_points[0], dict):
                    latest = data_points[0]
                    upper = latest.get('upper', 'N/A')
                    middle = latest.get('middle', 'N/A')
                    lower = latest.get('lower', 'N/A')
                    
                    band_width = upper - lower if isinstance(upper, (int, float)) and isinstance(lower, (int, float)) else 'N/A'
                    
                    analysis_text += "**Current Bollinger Band Values**:\n\n"
                    analysis_text += f"- Upper Band: {upper}\n"
                    analysis_text += f"- Middle Band: {middle}\n"
                    analysis_text += f"- Lower Band: {lower}\n"
                    
                    if band_width != 'N/A':
                        band_width_percent = (band_width / middle) * 100 if middle != 0 else 'N/A'
                        if band_width_percent != 'N/A':
                            analysis_text += f"- Band Width: {band_width} ({band_width_percent:.2f}% of middle band)\n\n"
                            
                            if band_width_percent > 5:  # Arbitrary threshold
                                analysis_text += "The bands are currently wide, indicating high volatility in the market.\n"
                            elif band_width_percent < 2:  # Arbitrary threshold
                                analysis_text += "The bands are currently tight, indicating low volatility. This often precedes a significant price move (volatility expansion).\n"
                            else:
                                analysis_text += "The bands are showing moderate volatility.\n"
                
                analysis_text += "\n### Trading Strategies\n"
                analysis_text += "Common Bollinger Bands trading strategies:\n\n"
                analysis_text += "1. **Bollinger Bounce**: Buy near the lower band and sell near the upper band in ranging markets.\n"
                analysis_text += "2. **Bollinger Squeeze**: Watch for periods of low volatility (narrow bands) followed by a breakout.\n"
                analysis_text += "3. **Riding the Bands**: In strong trends, prices can 'ride the bands' - stay long while price hugs the upper band in uptrends, or stay short while it hugs the lower band in downtrends.\n"
                analysis_text += "4. **W-Bottoms and M-Tops**: Look for W-shaped patterns at the lower band (bullish) or M-shaped patterns at the upper band (bearish).\n"
                
            elif indicator == "bbw":
                analysis_text += "### Bollinger Band Width Interpretation\n"
                analysis_text += "Bollinger Band Width (BBW) measures the distance between the upper and lower Bollinger Bands, normalized by the middle band.\n"
                analysis_text += "It is a direct indicator of market volatility, independent of price direction:\n\n"
                analysis_text += "- High BBW values indicate high volatility (expanded bands)\n"
                analysis_text += "- Low BBW values indicate low volatility (contracted bands)\n"
                analysis_text += "- Extremely low BBW often precedes significant price movements\n\n"
                
                if data_points and len(data_points) >= 5:
                    current_bbw = data_points[0]
                    bbw_5_period_avg = sum(data_points[:5]) / 5
                    
                    analysis_text += "**Current Bollinger Band Width Analysis**:\n\n"
                    analysis_text += f"- Current BBW: {current_bbw}\n"
                    analysis_text += f"- 5-period Average BBW: {bbw_5_period_avg:.6f}\n\n"
                    
                    if current_bbw < bbw_5_period_avg * 0.8:  # Arbitrary threshold
                        analysis_text += "The current BBW is significantly below the recent average, suggesting a period of consolidation or low volatility.\n"
                        analysis_text += "This often precedes a significant price movement or breakout, though the direction is not predictable from BBW alone.\n"
                    elif current_bbw > bbw_5_period_avg * 1.2:  # Arbitrary threshold
                        analysis_text += "The current BBW is significantly above the recent average, indicating increased volatility.\n"
                        analysis_text += "This often occurs during strong trends, breakouts, or market uncertainty.\n"
                    else:
                        analysis_text += "The current BBW is near the recent average, suggesting normal volatility conditions.\n"
                
                analysis_text += "\n### Trading Strategies\n"
                analysis_text += "Common Bollinger Band Width trading strategies:\n\n"
                analysis_text += "1. **Volatility Breakout**: Look for periods of extremely low BBW followed by an increase, which often signals the start of a new trend.\n"
                analysis_text += "2. **Volatility Cycle Trading**: Trade based on the cyclical nature of volatility (periods of high volatility are typically followed by low volatility, and vice versa).\n"
                analysis_text += "3. **Combining with Other Indicators**: Use BBW in conjunction with trend indicators to confirm the strength of a trend.\n"
                analysis_text += "4. **Mean Reversion**: Extremely high BBW values often revert to the mean, indicating potential trend exhaustion.\n"
        
        # Add multi-timeframe analysis if requested
        if multi_timeframe_analysis and indicator in ["rsi", "ma", "ema"]:
            analysis_text += "\n## Multi-Timeframe Analysis\n"
            analysis_text += "Professional traders always analyze multiple timeframes to confirm signals and understand market structure:\n\n"
            
            # Higher timeframe analysis
            higher_tf = ""
            if interval == "3m" or interval == "5m": higher_tf = "15m"
            elif interval == "15m": higher_tf = "1h"
            elif interval == "30m": higher_tf = "4h"
            elif interval == "1h": higher_tf = "4h"
            elif interval == "4h": higher_tf = "1d"
            elif interval == "1d": higher_tf = "1w"
            elif interval == "1w": higher_tf = "1M"
            
            # Lower timeframe analysis
            lower_tf = ""
            if interval == "15m": lower_tf = "5m"
            elif interval == "30m": lower_tf = "5m"
            elif interval == "1h": lower_tf = "15m"
            elif interval == "4h": lower_tf = "1h"
            elif interval == "1d": lower_tf = "4h"
            elif interval == "1w": lower_tf = "1d"
            elif interval == "1M": lower_tf = "1w"
            
            if higher_tf:
                analysis_text += f"### Higher Timeframe ({higher_tf})\n"
                analysis_text += f"Analyzing the {higher_tf} timeframe provides context for your {interval} signals:\n\n"
                analysis_text += "- **Trend Direction**: The higher timeframe defines the primary trend direction.\n"
                analysis_text += "- **Key Levels**: Major support/resistance zones are more reliable on higher timeframes.\n"
                analysis_text += "- **Momentum Confirmation**: Higher timeframe indicators should confirm your trading direction.\n\n"
                analysis_text += f"Institutional traders typically use {higher_tf} to determine strategic positioning and {interval} for tactical execution.\n"
            
            if lower_tf:
                analysis_text += f"\n### Lower Timeframe ({lower_tf})\n"
                analysis_text += f"Examining the {lower_tf} timeframe helps with entry/exit precision:\n\n"
                analysis_text += "- **Entry Timing**: Lower timeframe indicators help pinpoint optimal entry points.\n"
                analysis_text += "- **Exit Timing**: They also assist in identifying early reversal signals for profit-taking.\n"
                analysis_text += "- **Stop Placement**: More precise stop-loss placement based on local swing points.\n"
            
            analysis_text += "\n### Timeframe Alignment Strategy\n"
            analysis_text += "For highest probability trades, seek alignment across three timeframes:\n\n"
            analysis_text += f"1. **Strategic Direction**: Higher timeframe ({higher_tf}) confirms the primary trend.\n"
            analysis_text += f"2. **Tactical Entry**: Current timeframe ({interval}) identifies the setup and trigger.\n"
            analysis_text += f"3. **Precision Execution**: Lower timeframe ({lower_tf}) optimizes entry and exit points.\n\n"
            analysis_text += "When all three timeframes align, institutional traders typically deploy larger position sizes.\n"
        
        # Add market context if requested
        if include_market_context:
            analysis_text += "\n## Market Context & Correlation Analysis\n"
            analysis_text += "Professional traders never analyze an asset in isolation. Understanding broader market context is essential:\n\n"
            
            analysis_text += "### Market Regime\n"
            analysis_text += "Current market conditions significantly impact indicator effectiveness:\n\n"
            analysis_text += "- **Risk-On Environment**: During bull markets, oscillators like RSI tend to remain elevated longer. Consider adjusting overbought thresholds higher (e.g., 80 instead of 70).\n"
            analysis_text += "- **Risk-Off Environment**: During bear markets, oscillators like RSI tend to remain lower for extended periods. Consider adjusting oversold thresholds lower (e.g., 20 instead of 30).\n"
            analysis_text += "- **Volatility Environment**: During high volatility periods, indicator signals may trigger more frequently but with lower reliability. Consider using longer lookback periods or applying additional filters.\n\n"
            
            analysis_text += "### Correlation Considerations\n"
            analysis_text += "Key relationships that institutional traders monitor alongside technical indicators:\n\n"
            analysis_text += "- **BTC Correlation**: Most cryptocurrency tokens show significant correlation with Bitcoin. Always check BTC's trend and key levels.\n"
            analysis_text += "- **Sector Rotation**: Tokens in the same sector (DeFi, nft, Layer 1, etc.) often move together. Analyze sector trends for additional context.\n"
            analysis_text += "- **Market Dominance**: Changes in the dominance of major cryptocurrencies can signal rotations between large-cap and small-cap tokens.\n"
            analysis_text += "- **Inter-market Analysis**: Traditional markets (equities, bonds, dollar index) increasingly influence crypto. Consider their impact during important economic events.\n\n"
            
            analysis_text += "### Risk Management Framework\n"
            analysis_text += "Professional position sizing based on market conditions:\n\n"
            analysis_text += "- **Position Sizing**: In trending markets with clear signals, position size can be 1-2% of capital. In choppy or unclear conditions, reduce to 0.5-1%.\n"
            analysis_text += "- **Scaling**: Consider scaling into positions (25-50% at a time) rather than taking full positions at once.\n"
            analysis_text += "- **Correlation-Based Risk**: Reduce position size when trading multiple correlated assets to avoid overexposure to the same risk factor.\n"
            analysis_text += "- **Stop Placement**: In volatile markets, use ATR-based stops (2-3x ATR) rather than fixed percentages to account for asset-specific volatility.\n"
        
        # Add pattern detection if requested
        if detect_patterns and indicator not in ["macd", "bb", "bbw"]:
            analysis_text += "\n## Advanced Chart Pattern Analysis\n"
            analysis_text += "Chart patterns often form in technical indicators before they appear in price, providing early signals:\n\n"
            
            # This would use the pattern detection logic from our JavaScript function
            # For demonstration purposes, we'll add some common patterns
            
            analysis_text += "### Common Indicator Patterns\n"
            analysis_text += "- **Double Tops/Bottoms**: Two peaks/troughs at similar levels signal potential reversals.\n"
            analysis_text += "- **Head and Shoulders**: Three peaks with the middle one higher (or lower for inverse) indicate trend changes.\n"
            analysis_text += "- **Falling/Rising Wedges**: Converging trendlines with declining/rising indicator values suggest continuation or reversal.\n"
            analysis_text += "- **Indicator Trendlines**: Support/resistance lines on indicators often break before price trendlines.\n\n"
            
            analysis_text += "### Wyckoff Method Integration\n"
            analysis_text += "Professional traders often combine indicator analysis with Wyckoff market phases:\n\n"
            analysis_text += "1. **Accumulation**: Indicators show divergences and bottoming patterns as smart money accumulates.\n"
            analysis_text += "2. **Markup**: Indicators maintain strong readings with shallow pullbacks during this phase.\n"
            analysis_text += "3. **Distribution**: Look for weakening momentum and bearish divergences as smart money distributes.\n"
            analysis_text += "4. **Markdown**: Indicators maintain weak readings with short-lived bounces during this phase.\n\n"
            
            analysis_text += "### Harmonic Patterns\n"
            analysis_text += "Precise mathematical relationships that appear in both price and indicators:\n\n"
            analysis_text += "- **Fibonacci Relationships**: Key retracement levels (38.2%, 50%, 61.8%) often coincide with indicator reversals.\n"
            analysis_text += "- **ABCD Pattern**: Equal price swings often correlate with similar indicator movements.\n"
            analysis_text += "- **Gartley, Butterfly, Bat Patterns**: These advanced patterns can form in indicators, providing early reversal warnings.\n"
        
        # Add volume profile and market structure if professional analysis
        if professional_analysis:
            analysis_text += "\n## Institutional Trading Context\n"
            analysis_text += "Professional firms incorporate these additional dimensions when analyzing indicators:\n\n"
            
            analysis_text += "### Order Flow Analysis\n"
            analysis_text += "- **Volume Profile**: Areas of high historical volume create support/resistance zones that can impact indicator effectiveness.\n"
            analysis_text += "- **Market Depth**: Thin order books amplify indicator movements; thick order books absorb them.\n"
            analysis_text += "- **Smart Money Movement**: Large transactions and wallet movements by known entities often precede significant indicator signals.\n\n"
            
            analysis_text += "### Market Microstructure\n"
            analysis_text += "- **Liquidity Pools**: Identification of liquidity pools where stop orders cluster creates high-probability price targets.\n"
            analysis_text += "- **Inefficiency Zones**: Areas of rapid price movement with little trading activity create vacuum zones for price to revisit.\n"
            analysis_text += "- **Order Block Theory**: The last position taken before a significant move creates a zone of interest for future reversals.\n\n"
            
            analysis_text += "### Options Market Integration\n"
            analysis_text += "- **Options Volume**: Unusual options activity often precedes significant moves in underlying assets.\n"
            analysis_text += "- **Open Interest**: Growing open interest confirms trend strength in the direction of indicator signals.\n"
            analysis_text += "- **Put/Call Ratio**: Extreme readings align with indicator overbought/oversold signals to confirm potential reversals.\n"
        
        # Add a section on limitations and proper usage
        analysis_text += "\n## Important Considerations\n"
        analysis_text += "When using technical indicators for trading or investment decisions, keep in mind:\n\n"
        analysis_text += "1. **No Single Indicator is Perfect**: Always use multiple indicators and analysis methods.\n"
        analysis_text += "2. **Context Matters**: Consider the broader market conditions, fundamentals, and multiple timeframes.\n"
        analysis_text += "3. **False Signals**: All indicators can produce false signals, especially in choppy or sideways markets.\n"
        analysis_text += "4. **Lagging Nature**: Most indicators are based on past price data and inherently lag behind price action.\n"
        analysis_text += "5. **Risk Management**: Always use proper risk management regardless of indicator signals.\n"
        
        # Add disclaimer
        analysis_text += "\n## Disclaimer\n"
        analysis_text += "This analysis is for informational purposes only and not financial advice. "
        analysis_text += "Technical indicators have limitations and can produce false signals. "
        analysis_text += "Always do your own research and consider multiple factors before making investment decisions.\n"
        
        # Add pagination information
        if page_data['total_pages'] > 1:
            analysis_text += format_pagination_info(page_data)
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        results = []
        
        if should_chunk:
            chunks = chunk_text(analysis_text)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=analysis_text))
        
        return results
        
    except Exception as e:
        logger.error(f"Error in token_price_indicators: {e}")
        logger.error(traceback.format_exc())
        
        error_text = "# Error Retrieving Token Price Indicators\n\n"
        error_text += f"An error occurred while retrieving token price indicators: {str(e)}\n"
        
        # Get the full token database to help user
        all_tokens = await get_asset_database()
        
        if all_tokens:
            error_text += "\n## Available Tokens\n"
            error_text += "Here are some popular tokens you can try:\n\n"
            
            error_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
            error_text += "|------|--------|-----------|----------|----------|\n"
            
            # Show top 15 tokens by quantity
            sorted_tokens = sorted(all_tokens, key=lambda x: x.get('quantity', 0) or 0, reverse=True)
            for token in sorted_tokens[:15]:
                name = token.get('registry_name') or token.get('name_small') or 'Unknown'
                ticker = token.get('registry_ticker', '')
                policy = token.get('policy', '')
                hex_name = token.get('name', '')
                full_unit = f"{policy}{hex_name}"
                
                error_text += f"| {name} | {ticker} | {policy[:8]}... | {hex_name} | {full_unit[:16]}... |\n"
        
        return [types.TextContent(type="text", text=error_text)]

@mcp.tool(name="active_loans_analysis", description="Analyze active P2P loans for a specific Cardano token with risk assessment and market insights.")
async def active_loans_analysis(
    token_identifier: str,
    include: str = "collateral,debt",
    sort_by: str = "time",
    order: str = "desc",
    loans_count: int = 100,
    include_risk_analysis: bool = True,
    include_market_insights: bool = True,
    include_protocol_comparison: bool = True,
    liquidity_analysis: bool = True,
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    page_size: int = None,
    summary_only: bool = False,
    chunk_output: bool = None
) -> List[types.TextContent]:
    """
    Analyze active P2P loans for a specific Cardano token with risk assessment and market insights.
    
    Args:
        token_identifier: Token identifier (policy ID or full unit)
        include: Filter to loans where token is used as 'collateral', 'debt', 'interest' or a mix
        sort_by: What to sort results by ('time' or 'expiration')
        order: Sort direction ('asc' or 'desc')
        loans_count: Number of loan entries to analyze (max 100)
        include_risk_analysis: Whether to include detailed risk assessment
        include_market_insights: Whether to include market trend analysis
        include_protocol_comparison: Whether to include protocol performance comparison
        liquidity_analysis: Whether to include liquidity and capital efficiency analysis
        page: Page number for pagination
        page_size: Items per page (default: from global config)
        summary_only: Return only a summary without loan details
        chunk_output: Whether to split large output into chunks
    """
    global redis_cache
    try:
        # Use token_identifier directly - no resolution logic
        unit = token_identifier
        logger.info(f"Using token identifier directly: {unit}")
        
        # Create cache key including filters
        cache_key = f"{unit}:{include}:{sort_by}:{order}"
        
        # Try to get cached data first
        loans_data = await redis_cache.get_cached_data(
            table='cache_active_loans',
            key_field='cache_key',
            key_value=cache_key,
            limit=loans_count
        )
        
        api_error = None
        # If not in cache, try to fetch from API
        if not loans_data:
            logger.info(f"No cached active loans found, fetching from TapTools API")
            
            # Calculate how many pages to fetch
            pages_to_fetch = (loans_count + 99) // 100  # Ceiling division
            pages_to_fetch = min(pages_to_fetch, 1)  # Start with just one page
            
            all_loans = []
            try:
                for page_num in range(1, pages_to_fetch + 1):
                    loans_result, error = await taptools_api.get_active_loans(
                        unit=unit,
                        include=include,
                        sort_by=sort_by,
                        order=order,
                        page=page_num,
                        per_page=100
                    )
                    
                    if error:
                        api_error = error
                        logger.warning(f"API error on page {page_num}: {error}")
                        break
                    
                    if loans_result:
                        all_loans.extend(loans_result)
                        
                        # Apply rate limiting between pages
                        if page_num < pages_to_fetch:
                            await asyncio.sleep(0.2)  # 200ms delay between pages
                    
                    # Stop if we have enough loans
                    if len(all_loans) >= loans_count:
                        break
                
                # Cache the results if we got any
                if all_loans:
                    # Add cache_key to each loan for caching
                    for loan in all_loans:
                        loan['cache_key'] = cache_key
                    
                    redis_cache.store_cached_data(
                        table='cache_active_loans',
                        key_field='cache_key',
                        key_value=cache_key,
                        data_items=all_loans,
                        validator_function=validate_active_loan_data,
                        ttl_hours=1  # Cache for just 1 hour since loan status changes frequently
                    )
                    
                    loans_data = all_loans
            except Exception as fetch_error:
                logger.error(f"Error during API fetch: {fetch_error}")
                api_error = str(fetch_error)
        
        # If no data was found, return the token database to help the user
        if not loans_data or len(loans_data) == 0:
            # Get the full token database
            all_tokens = await get_asset_database()
            
            # Create response text
            analysis_text = "# Active Loans Analysis\n\n"
            
            if api_error:
                analysis_text += f"## Error Retrieving Active Loans\n"
                analysis_text += f"Unable to retrieve active loans from TapTools API.\n"
                analysis_text += f"Error: {api_error}\n\n"
                analysis_text += f"This could be due to:\n"
                analysis_text += f"- The token may not have any active loans\n"
                analysis_text += f"- The API endpoint may be temporarily unavailable\n"
                analysis_text += f"- The token identifier may be incorrect\n\n"
            else:
                analysis_text += f"## No Active Loans Found\n"
                analysis_text += f"No active loans were found for token: {token_identifier}\n\n"
                analysis_text += f"This token may not currently be used in P2P lending platforms like Lenfi or Levvy.\n\n"
            
            # Return the entire token database for reference
            analysis_text += "## Token Database\n"
            analysis_text += "Here's the complete token database. Use the policy ID + hex name for exact matching:\n\n"
            
            analysis_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
            analysis_text += "|------|--------|-----------|----------|----------|\n"
            
            # Limit the display to keep response size reasonable
            display_tokens = all_tokens[:30] if len(all_tokens) > 30 else all_tokens
            
            for token in display_tokens:
                name = token.get('registry_name') or token.get('name_small') or 'Unknown'
                ticker = token.get('registry_ticker', '')
                policy = token.get('policy', '')
                hex_name = token.get('name', '')
                full_unit = f"{policy}{hex_name}"
                
                analysis_text += f"| {name} | {ticker} | {policy[:8]}... | {hex_name} | {full_unit[:16]}... |\n"
            
            if len(all_tokens) > 30:
                analysis_text += f"\n*Showing 30 of {len(all_tokens)} tokens*\n"
            
            # Add instructions
            analysis_text += "\n## Usage Instructions\n"
            analysis_text += "1. Find your desired token in the table above\n"
            analysis_text += "2. Copy the 'Full Unit' value (policy ID + hex name)\n"
            analysis_text += "3. Retry with that exact value as the token_identifier\n"
            analysis_text += "4. Ensure the token is being used in P2P lending platforms\n"
            
            return [types.TextContent(type="text", text=analysis_text)]
        
        # Process the loans data to extract key metrics
        protocols = {}
        total_loans = len(loans_data)
        total_collateral_value = 0
        total_debt_value = 0
        total_interest_value = 0
        health_factors = []
        loan_durations = []
        interest_rates = []
        loan_sizes = []
        at_risk_loans = 0
        
        # Track tokens used in the loans
        collateral_tokens = {}
        debt_tokens = {}
        interest_tokens = {}
        
        current_time = int(time.time())
        
        for loan in loans_data:
            # Track protocols
            protocol = loan.get('protocol', 'Unknown')
            protocols[protocol] = protocols.get(protocol, 0) + 1
            
            # Track values
            collateral_value = float(loan.get('collateralValue', 0))
            debt_value = float(loan.get('debtValue', 0))
            interest_value = float(loan.get('interestValue', 0))
            
            total_collateral_value += collateral_value
            total_debt_value += debt_value
            total_interest_value += interest_value
            
            # Track health factors
            health = float(loan.get('health', 0))
            health_factors.append(health)
            
            # Count at-risk loans (health factor < 1.1 is typically considered risky)
            if health < 1.1:
                at_risk_loans += 1
            
            # Track loan durations
            expiration = int(loan.get('expiration', 0))
            loan_time = int(loan.get('time', 0))
            if expiration > 0 and loan_time > 0:
                duration = expiration - loan_time
                loan_durations.append(duration)
                
                # Calculate days remaining
                days_remaining = (expiration - current_time) / 86400  # Convert seconds to days
                loan['days_remaining'] = days_remaining
            
            # Track interest rates
            if debt_value > 0 and 'interestValue' in loan:
                interest_rate = (interest_value / debt_value) * 100
                if duration > 0:
                    # Annualized interest rate
                    annual_rate = (interest_rate / (duration / 86400)) * 365
                    interest_rates.append(annual_rate)
                    loan['annual_interest_rate'] = annual_rate
            
            # Track loan sizes
            loan_sizes.append(debt_value)
            
            # Track tokens used
            collateral_token = loan.get('collateralToken', 'Unknown')
            debt_token = loan.get('debtToken', 'Unknown')
            interest_token = loan.get('interestToken', 'Unknown')
            
            if collateral_token != 'Unknown':
                collateral_tokens[collateral_token] = collateral_tokens.get(collateral_token, 0) + 1
                
            if debt_token != 'Unknown':
                debt_tokens[debt_token] = debt_tokens.get(debt_token, 0) + 1
                
            if interest_token != 'Unknown':
                interest_tokens[interest_token] = interest_tokens.get(interest_token, 0) + 1
        
        # Calculate average values
        avg_health_factor = sum(health_factors) / len(health_factors) if health_factors else 0
        avg_loan_duration = sum(loan_durations) / len(loan_durations) if loan_durations else 0
        avg_interest_rate = sum(interest_rates) / len(interest_rates) if interest_rates else 0
        avg_loan_size = sum(loan_sizes) / len(loan_sizes) if loan_sizes else 0
        
        # Generate analysis text
        analysis_text = f"# Active P2P Loans Analysis for {token_identifier}\n\n"
        
        # Add summary information
        analysis_text += "## Summary Statistics\n"
        analysis_text += f"- Total Active Loans: {total_loans}\n"
        analysis_text += f"- Total Collateral Value: ${total_collateral_value:,.2f}\n"
        analysis_text += f"- Total Debt Value: ${total_debt_value:,.2f}\n"
        analysis_text += f"- Total Interest Value: ${total_interest_value:,.2f}\n"
        analysis_text += f"- Average Health Factor: {avg_health_factor:.2f}\n"
        
        if loan_durations:
            avg_days = avg_loan_duration / 86400  # Convert seconds to days
            analysis_text += f"- Average Loan Duration: {avg_days:.1f} days\n"
        
        if interest_rates:
            analysis_text += f"- Average Annual Interest Rate: {avg_interest_rate:.2f}%\n"
        
        analysis_text += f"- Average Loan Size: ${avg_loan_size:,.2f}\n"
        analysis_text += f"- At-Risk Loans (Health < 1.1): {at_risk_loans} ({(at_risk_loans/total_loans*100) if total_loans > 0 else 0:.1f}%)\n"
        
        if protocols:
            analysis_text += f"\n### Protocols\n"
            for protocol, count in protocols.items():
                percentage = (count / total_loans) * 100
                analysis_text += f"- {protocol}: {count} loans ({percentage:.1f}%)\n"
        
        # If summary_only is True, return just the summary
        if summary_only:
            if health_factors:
                min_health = min(health_factors)
                max_health = max(health_factors)
                analysis_text += f"\n### Health Factor Distribution\n"
                analysis_text += f"- Minimum: {min_health:.2f}\n"
                analysis_text += f"- Maximum: {max_health:.2f}\n"
                analysis_text += f"- Average: {avg_health_factor:.2f}\n"
                
                # Add health factor risk assessment
                if avg_health_factor < 1.2:
                    analysis_text += "\n**Risk Assessment**: High risk. The average health factor is critically low, indicating potential liquidation risk across many loans.\n"
                elif avg_health_factor < 1.5:
                    analysis_text += "\n**Risk Assessment**: Moderate risk. The average health factor indicates adequate collateralization but limited safety margin.\n"
                else:
                    analysis_text += "\n**Risk Assessment**: Low risk. The average health factor suggests well-collateralized positions with good safety margins.\n"
            
            if interest_rates:
                min_rate = min(interest_rates)
                max_rate = max(interest_rates)
                analysis_text += f"\n### Interest Rate Analysis\n"
                analysis_text += f"- Minimum Annual Rate: {min_rate:.2f}%\n"
                analysis_text += f"- Maximum Annual Rate: {max_rate:.2f}%\n"
                analysis_text += f"- Average Annual Rate: {avg_interest_rate:.2f}%\n"
                
                # Add interest rate assessment
                if avg_interest_rate > 30:
                    analysis_text += "\n**Market Assessment**: Premium rates. Interest rates are exceptionally high, indicating significant perceived risk or high demand for this asset.\n"
                elif avg_interest_rate > 15:
                    analysis_text += "\n**Market Assessment**: Above-average rates. Interest rates suggest moderate demand with some perceived risk.\n"
                else:
                    analysis_text += "\n**Market Assessment**: Standard rates. Interest rates align with typical DeFi lending markets, suggesting stable demand and low perceived risk.\n"
            
            return [types.TextContent(type="text", text=analysis_text)]
        
        # Apply pagination for detailed loan list
        effective_page_size = get_config_value("page_size", page_size)
        paginator = PaginatedResponse(loans_data, effective_page_size)
        page_data = paginator.get_page(page)
        
        # Update analysis text with pagination info if needed
        if page_data['total_pages'] > 1:
            analysis_text += f"\nShowing page {page_data['page']} of {page_data['total_pages']} "
            analysis_text += f"(total loans: {page_data['total_items']})\n\n"
        
        # Add health factor distribution if risk analysis is requested
        if include_risk_analysis:
            analysis_text += "\n## Risk Analysis\n"
            
            # Calculate health factor distribution
            health_categories = {
                "Critical Risk (< 1.05)": 0,
                "High Risk (1.05-1.1)": 0,
                "Moderate Risk (1.1-1.2)": 0,
                "Low Risk (1.2-1.5)": 0,
                "Very Safe (> 1.5)": 0
            }
            
            for health in health_factors:
                if health < 1.05:
                    health_categories["Critical Risk (< 1.05)"] += 1
                elif health < 1.1:
                    health_categories["High Risk (1.05-1.1)"] += 1
                elif health < 1.2:
                    health_categories["Moderate Risk (1.1-1.2)"] += 1
                elif health < 1.5:
                    health_categories["Low Risk (1.2-1.5)"] += 1
                else:
                    health_categories["Very Safe (> 1.5)"] += 1
            
            analysis_text += "### Health Factor Distribution\n"
            analysis_text += "| Risk Category | Count | Percentage | Liquidation Risk |\n"
            analysis_text += "|---------------|-------|------------|------------------|\n"
            
            for category, count in health_categories.items():
                percentage = (count / total_loans) * 100 if total_loans > 0 else 0
                
                risk_assessment = ""
                if "Critical" in category:
                    risk_assessment = "Imminent"
                elif "High" in category:
                    risk_assessment = "Very likely"
                elif "Moderate" in category:
                    risk_assessment = "Possible"
                elif "Low" in category:
                    risk_assessment = "Unlikely"
                else:
                    risk_assessment = "Very unlikely"
                
                analysis_text += f"| {category} | {count} | {percentage:.1f}% | {risk_assessment} |\n"
            
            # Add overall risk assessment
            analysis_text += "\n### Risk Assessment\n"
            critical_high_risk = health_categories["Critical Risk (< 1.05)"] + health_categories["High Risk (1.05-1.1)"]
            critical_high_percentage = (critical_high_risk / total_loans) * 100 if total_loans > 0 else 0
            
            if critical_high_percentage > 25:
                analysis_text += f"**High Market Risk**: {critical_high_percentage:.1f}% of loans are at high or critical risk of liquidation. This indicates significant market stress for this asset.\n\n"
                analysis_text += "**Recommendation**: Exercise extreme caution. Liquidation cascades are possible if market conditions deteriorate further.\n"
            elif critical_high_percentage > 10:
                analysis_text += f"**Elevated Market Risk**: {critical_high_percentage:.1f}% of loans are at high or critical risk of liquidation. This suggests caution is warranted.\n\n"
                analysis_text += "**Recommendation**: Monitor closely. Consider reducing exposure to this asset in lending platforms.\n"
            else:
                analysis_text += f"**Normal Market Risk**: Only {critical_high_percentage:.1f}% of loans are at high or critical risk of liquidation. The market appears relatively stable.\n\n"
                analysis_text += "**Recommendation**: Standard vigilance. Current risk levels are within normal parameters.\n"
            
            # Add liquidation impact analysis
            if critical_high_risk > 0:
                at_risk_collateral = 0
                for loan in loans_data:
                    health = float(loan.get('health', 0))
                    if health < 1.1:
                        at_risk_collateral += float(loan.get('collateralValue', 0))
                
                analysis_text += f"\n### Potential Liquidation Impact\n"
                analysis_text += f"- Total At-Risk Collateral: ${at_risk_collateral:,.2f}\n"
                analysis_text += f"- Percentage of Total Collateral: {(at_risk_collateral/total_collateral_value*100) if total_collateral_value > 0 else 0:.1f}%\n\n"
                
                if at_risk_collateral > total_collateral_value * 0.2:
                    analysis_text += "**Severe Impact**: A significant amount of collateral is at risk. Liquidations could cause substantial market pressure.\n"
                elif at_risk_collateral > total_collateral_value * 0.1:
                    analysis_text += "**Moderate Impact**: A notable amount of collateral is at risk. Liquidations could cause moderate market pressure.\n"
                else:
                    analysis_text += "**Limited Impact**: A small amount of collateral is at risk. Liquidations would likely have minimal market impact.\n"
                    
                # Estimate price impact of liquidations
                analysis_text += f"\n**Estimated Price Impact**: If all at-risk positions were liquidated simultaneously, the price impact could range from {min(20, (at_risk_collateral/total_collateral_value*100)):.1f}% to {min(40, (at_risk_collateral/total_collateral_value*200)):.1f}%, depending on market depth and liquidation mechanisms.\n"
        
        # Add market insights if requested
        if include_market_insights:
            analysis_text += "\n## Market Insights\n"
            
            # Interest rate analysis
            if interest_rates:
                min_rate = min(interest_rates)
                max_rate = max(interest_rates)
                median_rate = sorted(interest_rates)[len(interest_rates)//2] if interest_rates else 0
                
                analysis_text += "### Interest Rate Analysis\n"
                analysis_text += f"- Minimum Annual Rate: {min_rate:.2f}%\n"
                analysis_text += f"- Maximum Annual Rate: {max_rate:.2f}%\n"
                analysis_text += f"- Median Annual Rate: {median_rate:.2f}%\n"
                analysis_text += f"- Average Annual Rate: {avg_interest_rate:.2f}%\n\n"
                
                # Add interest rate distribution
                rate_buckets = {
                    "< 5%": 0,
                    "5-10%": 0,
                    "10-15%": 0,
                    "15-20%": 0,
                    "20-30%": 0,
                    "> 30%": 0
                }
                
                for rate in interest_rates:
                    if rate < 5:
                        rate_buckets["< 5%"] += 1
                    elif rate < 10:
                        rate_buckets["5-10%"] += 1
                    elif rate < 15:
                        rate_buckets["10-15%"] += 1
                    elif rate < 20:
                        rate_buckets["15-20%"] += 1
                    elif rate < 30:
                        rate_buckets["20-30%"] += 1
                    else:
                        rate_buckets["> 30%"] += 1
                
                analysis_text += "#### Interest Rate Distribution\n"
                analysis_text += "| Rate Range | Count | Percentage |\n"
                analysis_text += "|------------|-------|------------|\n"
                
                for bucket, count in rate_buckets.items():
                    percentage = (count / len(interest_rates)) * 100 if interest_rates else 0
                    analysis_text += f"| {bucket} | {count} | {percentage:.1f}% |\n"
                
                # Add interest rate interpretation
                analysis_text += "\n#### Interest Rate Interpretation\n"
                if avg_interest_rate > 25:
                    analysis_text += "The high average interest rate suggests:\n"
                    analysis_text += "- Significant perceived risk for this asset\n"
                    analysis_text += "- Strong demand for borrowing this token\n"
                    analysis_text += "- Potential for good yield farming opportunities for lenders\n"
                    analysis_text += "- Possible market stress or liquidity constraints\n"
                elif avg_interest_rate > 15:
                    analysis_text += "The above-average interest rate suggests:\n"
                    analysis_text += "- Moderate perceived risk for this asset\n"
                    analysis_text += "- Healthy demand for borrowing\n"
                    analysis_text += "- Competitive yield opportunities for lenders\n"
                elif avg_interest_rate > 7:
                    analysis_text += "The moderate interest rate suggests:\n"
                    analysis_text += "- Normal perceived risk for this asset\n"
                    analysis_text += "- Balanced supply and demand\n"
                    analysis_text += "- Standard yield opportunities\n"
                else:
                    analysis_text += "The low interest rate suggests:\n"
                    analysis_text += "- Low perceived risk for this asset\n"
                    analysis_text += "- Abundant supply relative to demand\n"
                    analysis_text += "- Limited yield opportunities for lenders\n"
            
            # Loan duration analysis
            if loan_durations:
                min_duration = min(loan_durations) / 86400  # Convert to days
                max_duration = max(loan_durations) / 86400
                median_duration = sorted(loan_durations)[len(loan_durations)//2] / 86400 if loan_durations else 0
                
                analysis_text += "\n### Loan Duration Analysis\n"
                analysis_text += f"- Minimum Duration: {min_duration:.1f} days\n"
                analysis_text += f"- Maximum Duration: {max_duration:.1f} days\n"
                analysis_text += f"- Median Duration: {median_duration:.1f} days\n"
                analysis_text += f"- Average Duration: {avg_loan_duration/86400:.1f} days\n\n"
                
                # Add duration distribution
                duration_buckets = {
                    "< 7 days": 0,
                    "7-14 days": 0,
                    "14-30 days": 0,
                    "30-60 days": 0,
                    "60-90 days": 0,
                    "> 90 days": 0
                }
                
                for duration in loan_durations:
                    days = duration / 86400
                    if days < 7:
                        duration_buckets["< 7 days"] += 1
                    elif days < 14:
                        duration_buckets["7-14 days"] += 1
                    elif days < 30:
                        duration_buckets["14-30 days"] += 1
                    elif days < 60:
                        duration_buckets["30-60 days"] += 1
                    elif days < 90:
                        duration_buckets["60-90 days"] += 1
                    else:
                        duration_buckets["> 90 days"] += 1
                
                analysis_text += "#### Duration Distribution\n"
                analysis_text += "| Duration Range | Count | Percentage |\n"
                analysis_text += "|----------------|-------|------------|\n"
                
                for bucket, count in duration_buckets.items():
                    percentage = (count / len(loan_durations)) * 100
                    analysis_text += f"| {bucket} | {count} | {percentage:.1f}% |\n"
                
                # Add duration interpretation
                analysis_text += "\n#### Duration Interpretation\n"
                
                short_term_pct = (duration_buckets["< 7 days"] + duration_buckets["7-14 days"]) / len(loan_durations) * 100
                long_term_pct = (duration_buckets["60-90 days"] + duration_buckets["> 90 days"]) / len(loan_durations) * 100
                
                if short_term_pct > 60:
                    analysis_text += "The prevalence of short-term loans suggests:\n"
                    analysis_text += "- Market participants prefer short-term exposure due to higher market uncertainty\n"
                    analysis_text += "- Potentially higher volatility expectations in the near term\n"
                    analysis_text += "- Borrowers may be using loans for short-term trading opportunities\n"
                    analysis_text += "- Higher rollover frequency and greater market activity\n"
                elif long_term_pct > 60:
                    analysis_text += "The prevalence of long-term loans suggests:\n"
                    analysis_text += "- Market participants are comfortable with longer exposure periods\n"
                    analysis_text += "- Potentially lower volatility expectations\n"
                    analysis_text += "- Borrowers may be using loans for longer-term strategies like yield farming\n"
                    analysis_text += "- Lower rollover frequency and more stable market conditions\n"
                else:
                    analysis_text += "The balanced distribution of loan durations suggests:\n"
                    analysis_text += "- Diverse market participant strategies\n"
                    analysis_text += "- Mixed outlook on market volatility\n"
                    analysis_text += "- Healthy ecosystem with various use cases for borrowed funds\n"
            
            # Loan size analysis
            if loan_sizes:
                min_size = min(loan_sizes)
                max_size = max(loan_sizes)
                median_size = sorted(loan_sizes)[len(loan_sizes)//2] if loan_sizes else 0
                
                analysis_text += "\n### Loan Size Analysis\n"
                analysis_text += f"- Minimum Loan Size: ${min_size:,.2f}\n"
                analysis_text += f"- Maximum Loan Size: ${max_size:,.2f}\n"
                analysis_text += f"- Median Loan Size: ${median_size:,.2f}\n"
                analysis_text += f"- Average Loan Size: ${avg_loan_size:,.2f}\n\n"
                
                # Check for loan size concentration
                large_loans_threshold = avg_loan_size * 3
                large_loans_count = sum(1 for size in loan_sizes if size > large_loans_threshold)
                large_loans_value = sum(size for size in loan_sizes if size > large_loans_threshold)
                large_loans_pct = (large_loans_count / len(loan_sizes)) * 100 if loan_sizes else 0
                large_loans_value_pct = (large_loans_value / total_debt_value) * 100 if total_debt_value > 0 else 0
                
                analysis_text += "#### Loan Size Distribution\n"
                if large_loans_count > 0:
                    analysis_text += f"- Large Loans (> ${large_loans_threshold:,.2f}): {large_loans_count} loans ({large_loans_pct:.1f}% of total)\n"
                    analysis_text += f"- Value of Large Loans: ${large_loans_value:,.2f} ({large_loans_value_pct:.1f}% of total debt)\n\n"
                    
                    if large_loans_value_pct > 70:
                        analysis_text += "**High Concentration Risk**: A small number of large loans represent the majority of borrowed value.\n"
                    elif large_loans_value_pct > 50:
                        analysis_text += "**Moderate Concentration Risk**: Large loans represent a significant portion of borrowed value.\n"
                    else:
                        analysis_text += "**Low Concentration Risk**: Large loans represent a minor portion of borrowed value.\n"
                else:
                    analysis_text += "**No Concentration Risk**: Loan sizes are relatively uniformly distributed.\n"
                
                # Add size categories for more detail
                size_buckets = {
                    "< $1,000": 0,
                    "$1,000-$5,000": 0,
                    "$5,000-$10,000": 0,
                    "$10,000-$50,000": 0,
                    "$50,000-$100,000": 0,
                    "> $100,000": 0
                }
                
                for size in loan_sizes:
                    if size < 1000:
                        size_buckets["< $1,000"] += 1
                    elif size < 5000:
                        size_buckets["$1,000-$5,000"] += 1
                    elif size < 10000:
                        size_buckets["$5,000-$10,000"] += 1
                    elif size < 50000:
                        size_buckets["$10,000-$50,000"] += 1
                    elif size < 100000:
                        size_buckets["$50,000-$100,000"] += 1
                    else:
                        size_buckets["> $100,000"] += 1
                
                analysis_text += "\n| Size Range | Count | Percentage |\n"
                analysis_text += "|------------|-------|------------|\n"
                
                for bucket, count in size_buckets.items():
                    percentage = (count / len(loan_sizes)) * 100 if loan_sizes else 0
                    analysis_text += f"| {bucket} | {count} | {percentage:.1f}% |\n"
                
                # Add size interpretation
                analysis_text += "\n#### Loan Size Interpretation\n"
                
                retail_pct = (size_buckets["< $1,000"] + size_buckets["$1,000-$5,000"]) / len(loan_sizes) * 100 if loan_sizes else 0
                institutional_pct = (size_buckets["$50,000-$100,000"] + size_buckets["> $100,000"]) / len(loan_sizes) * 100 if loan_sizes else 0
                
                if retail_pct > 70:
                    analysis_text += "The market is dominated by small/retail borrowers, suggesting:\n"
                    analysis_text += "- Broad accessibility and adoption among retail users\n"
                    analysis_text += "- Lower systemic risk from individual defaults\n"
                    analysis_text += "- Potentially higher operational costs per dollar lent\n"
                elif institutional_pct > 30:
                    analysis_text += "The market has significant institutional/large borrower participation, suggesting:\n"
                    analysis_text += "- Institutional confidence in the lending protocol\n"
                    analysis_text += "- Higher systemic risk from large position defaults\n"
                    analysis_text += "- Higher capital efficiency for the protocol\n"
                else:
                    analysis_text += "The market has a balanced mix of borrower sizes, suggesting:\n"
                    analysis_text += "- Healthy ecosystem with diverse participants\n"
                    analysis_text += "- Moderate and distributed risk profile\n"
                    analysis_text += "- Good protocol maturity and adoption across user segments\n"
            
            # Collateral-to-debt ratio analysis
            collateral_to_debt_ratios = []
            for loan in loans_data:
                collateral_value = float(loan.get('collateralValue', 0))
                debt_value = float(loan.get('debtValue', 0))
                if debt_value > 0:
                    ratio = collateral_value / debt_value
                    collateral_to_debt_ratios.append(ratio)
            
            if collateral_to_debt_ratios:
                avg_c2d_ratio = sum(collateral_to_debt_ratios) / len(collateral_to_debt_ratios)
                min_c2d_ratio = min(collateral_to_debt_ratios)
                max_c2d_ratio = max(collateral_to_debt_ratios)
                
                analysis_text += "\n### Collateralization Analysis\n"
                analysis_text += f"- Average Collateral-to-Debt Ratio: {avg_c2d_ratio:.2f}\n"
                analysis_text += f"- Minimum Collateral-to-Debt Ratio: {min_c2d_ratio:.2f}\n"
                analysis_text += f"- Maximum Collateral-to-Debt Ratio: {max_c2d_ratio:.2f}\n"
                analysis_text += f"- Total Collateral-to-Debt Ratio: {(total_collateral_value/total_debt_value) if total_debt_value > 0 else 0:.2f}\n\n"
                
                # Interpret collateralization levels
                if avg_c2d_ratio > 2.0:
                    analysis_text += "**High Collateralization**: The market shows very conservative collateral practices, suggesting:\n"
                    analysis_text += "- High caution among borrowers\n"
                    analysis_text += "- Potential perceived price volatility for collateral assets\n"
                    analysis_text += "- Strong protection for lenders\n"
                    analysis_text += "- Inefficient capital utilization\n"
                elif avg_c2d_ratio > 1.5:
                    analysis_text += "**Moderate Collateralization**: The market shows standard collateral practices, suggesting:\n"
                    analysis_text += "- Typical risk management by borrowers\n"
                    analysis_text += "- Normal perceived price stability for collateral assets\n"
                    analysis_text += "- Good protection for lenders with reasonable capital efficiency\n"
                else:
                    analysis_text += "**Tight Collateralization**: The market shows aggressive collateral practices, suggesting:\n"
                    analysis_text += "- Risk-taking behavior among borrowers\n"
                    analysis_text += "- High confidence in price stability for collateral assets\n"
                    analysis_text += "- Heightened liquidation risk during market volatility\n"
                    analysis_text += "- Maximum capital efficiency but lower safety margins\n"
        
        # Add protocol comparison if requested and multiple protocols exist
        if include_protocol_comparison and len(protocols) > 1:
            analysis_text += "\n## Protocol Comparison\n"
            
            # Group loans by protocol
            protocol_loans = {}
            for protocol in protocols.keys():
                protocol_loans[protocol] = []
            
            for loan in loans_data:
                protocol = loan.get('protocol', 'Unknown')
                if protocol in protocol_loans:
                    protocol_loans[protocol].append(loan)
            
            # Compare key metrics across protocols
            analysis_text += "### Key Metrics by Protocol\n"
            analysis_text += "| Protocol | Loans | Avg Health | Avg Interest Rate | Avg Loan Size | Total Value |\n"
            analysis_text += "|----------|-------|------------|-------------------|---------------|-------------|\n"
            
            for protocol, loans in protocol_loans.items():
                protocol_health_factors = [float(loan.get('health', 0)) for loan in loans]
                protocol_interest_rates = []
                protocol_loan_sizes = [float(loan.get('debtValue', 0)) for loan in loans]
                protocol_total_value = sum(protocol_loan_sizes)
                
                for loan in loans:
                    if 'annual_interest_rate' in loan:
                        protocol_interest_rates.append(loan['annual_interest_rate'])
                
                avg_protocol_health = sum(protocol_health_factors) / len(protocol_health_factors) if protocol_health_factors else 0
                avg_protocol_interest = sum(protocol_interest_rates) / len(protocol_interest_rates) if protocol_interest_rates else 0
                avg_protocol_loan_size = sum(protocol_loan_sizes) / len(protocol_loan_sizes) if protocol_loan_sizes else 0
                
                analysis_text += f"| {protocol} | {len(loans)} | {avg_protocol_health:.2f} | {avg_protocol_interest:.2f}% | ${avg_protocol_loan_size:,.2f} | ${protocol_total_value:,.2f} |\n"
            
            # Add protocol risk comparison
            analysis_text += "\n### Risk Profile Comparison\n"
            
            for protocol, loans in protocol_loans.items():
                protocol_health_factors = [float(loan.get('health', 0)) for loan in loans]
                at_risk_count = sum(1 for health in protocol_health_factors if health < 1.1)
                at_risk_pct = (at_risk_count / len(loans)) * 100 if loans else 0
                
                analysis_text += f"**{protocol}**:\n"
                analysis_text += f"- At-Risk Loans: {at_risk_count} ({at_risk_pct:.1f}%)\n"
                
                if avg_protocol_health < 1.2:
                    analysis_text += "- Risk Level: High\n"
                elif avg_protocol_health < 1.5:
                    analysis_text += "- Risk Level: Moderate\n"
                else:
                    analysis_text += "- Risk Level: Low\n"
            
            # Add protocol efficiency comparison
            if liquidity_analysis:
                analysis_text += "\n### Capital Efficiency Comparison\n"
                
                for protocol, loans in protocol_loans.items():
                    protocol_collateral_values = [float(loan.get('collateralValue', 0)) for loan in loans]
                    protocol_debt_values = [float(loan.get('debtValue', 0)) for loan in loans]
                    
                    protocol_total_collateral = sum(protocol_collateral_values)
                    protocol_total_debt = sum(protocol_debt_values)
                    
                    protocol_c2d_ratio = protocol_total_collateral / protocol_total_debt if protocol_total_debt > 0 else 0
                    
                    analysis_text += f"**{protocol}**:\n"
                    analysis_text += f"- Collateral-to-Debt Ratio: {protocol_c2d_ratio:.2f}\n"
                    analysis_text += f"- Capital Efficiency: {(1/protocol_c2d_ratio*100) if protocol_c2d_ratio > 0 else 0:.1f}%\n"
                    
                    if protocol_c2d_ratio > 2.0:
                        analysis_text += "- Efficiency Assessment: Low capital efficiency, conservative approach\n"
                    elif protocol_c2d_ratio > 1.5:
                        analysis_text += "- Efficiency Assessment: Moderate capital efficiency, balanced approach\n"
                    else:
                        analysis_text += "- Efficiency Assessment: High capital efficiency, aggressive approach\n"
        
        # Add liquidity analysis if requested
        if liquidity_analysis:
            analysis_text += "\n## Liquidity Analysis\n"
            
            # Total capital efficiency
            total_c2d_ratio = total_collateral_value / total_debt_value if total_debt_value > 0 else 0
            capital_efficiency = (1 / total_c2d_ratio * 100) if total_c2d_ratio > 0 else 0
            
            analysis_text += f"### Capital Utilization\n"
            analysis_text += f"- Total Collateral: ${total_collateral_value:,.2f}\n"
            analysis_text += f"- Total Debt: ${total_debt_value:,.2f}\n"
            analysis_text += f"- Collateral-to-Debt Ratio: {total_c2d_ratio:.2f}\n"
            analysis_text += f"- Capital Efficiency: {capital_efficiency:.1f}%\n\n"
            
            if capital_efficiency < 40:
                analysis_text += "**Low Capital Efficiency**: Significant collateral is locked relative to borrowed value. This suggests conservative risk parameters but potential for optimization.\n"
            elif capital_efficiency < 60:
                analysis_text += "**Moderate Capital Efficiency**: Reasonable balance between collateral requirements and borrowed value. This suggests appropriately calibrated risk parameters.\n"
            else:
                analysis_text += "**High Capital Efficiency**: Minimal excess collateral relative to borrowed value. This suggests aggressive risk parameters that maximize capital utilization but increase systemic risk.\n"
            
            # Token distribution analysis
            if len(collateral_tokens) > 0:
                analysis_text += "\n### Token Utilization\n"
                
                analysis_text += "#### Collateral Token Distribution\n"
                if len(collateral_tokens) > 1:
                    analysis_text += "| Token | Count | Percentage |\n"
                    analysis_text += "|-------|-------|------------|\n"
                    
                    for token, count in sorted(collateral_tokens.items(), key=lambda x: x[1], reverse=True):
                        percentage = (count / total_loans) * 100
                        token_display = token[:8] + "..." if len(token) > 10 else token
                        analysis_text += f"| {token_display} | {count} | {percentage:.1f}% |\n"
                    
                    # Add diversification assessment
                    top_collateral = max(collateral_tokens.values())
                    top_collateral_pct = (top_collateral / total_loans) * 100
                    
                    if top_collateral_pct > 80:
                        analysis_text += "\n**Low Collateral Diversification**: The market is heavily concentrated in a single collateral token.\n"
                    elif top_collateral_pct > 50:
                        analysis_text += "\n**Moderate Collateral Diversification**: The market shows some concentration in a dominant collateral token.\n"
                    else:
                        analysis_text += "\n**High Collateral Diversification**: The market shows good distribution across multiple collateral tokens.\n"
                else:
                    analysis_text += "Single collateral token detected. All loans use the same collateral.\n"
                
                if len(debt_tokens) > 1:
                    analysis_text += "\n#### Debt Token Distribution\n"
                    analysis_text += "| Token | Count | Percentage |\n"
                    analysis_text += "|-------|-------|------------|\n"
                    
                    for token, count in sorted(debt_tokens.items(), key=lambda x: x[1], reverse=True):
                        percentage = (count / total_loans) * 100
                        token_display = token[:8] + "..." if len(token) > 10 else token
                        analysis_text += f"| {token_display} | {count} | {percentage:.1f}% |\n"
                    
                    # Add diversification assessment
                    top_debt = max(debt_tokens.values())
                    top_debt_pct = (top_debt / total_loans) * 100
                    
                    if top_debt_pct > 80:
                        analysis_text += "\n**Low Debt Diversification**: Borrowing is heavily concentrated in a single token.\n"
                    elif top_debt_pct > 50:
                        analysis_text += "\n**Moderate Debt Diversification**: Borrowing shows some concentration in a dominant token.\n"
                    else:
                        analysis_text += "\n**High Debt Diversification**: Borrowing shows good distribution across multiple tokens.\n"
                else:
                    analysis_text += "\nSingle debt token detected. All loans borrow the same token.\n"
                
                # Add liquidity implications
                analysis_text += "\n### Liquidity Implications\n"
                
                if total_loans > 50:
                    analysis_text += "**High Market Activity**: The significant number of active loans indicates strong market participation and robust liquidity.\n"
                elif total_loans > 20:
                    analysis_text += "**Moderate Market Activity**: The number of active loans suggests reasonable market participation and adequate liquidity.\n"
                else:
                    analysis_text += "**Low Market Activity**: The limited number of active loans suggests lower market participation and potentially constrained liquidity.\n"
                
                # Loan expirations analysis
                upcoming_expirations = []
                for loan in loans_data:
                    if 'days_remaining' in loan:
                        upcoming_expirations.append({
                            'days': loan['days_remaining'],
                            'value': float(loan.get('debtValue', 0))
                        })
                
                if upcoming_expirations:
                    # Group by time periods
                    expiration_groups = {
                        "< 1 day": {"count": 0, "value": 0},
                        "1-3 days": {"count": 0, "value": 0},
                        "4-7 days": {"count": 0, "value": 0},
                        "8-14 days": {"count": 0, "value": 0},
                        "15-30 days": {"count": 0, "value": 0},
                        "> 30 days": {"count": 0, "value": 0}
                    }
                    
                    for exp in upcoming_expirations:
                        days = exp['days']
                        value = exp['value']
                        
                        if days < 1:
                            expiration_groups["< 1 day"]["count"] += 1
                            expiration_groups["< 1 day"]["value"] += value
                        elif days < 4:
                            expiration_groups["1-3 days"]["count"] += 1
                            expiration_groups["1-3 days"]["value"] += value
                        elif days < 8:
                            expiration_groups["4-7 days"]["count"] += 1
                            expiration_groups["4-7 days"]["value"] += value
                        elif days < 15:
                            expiration_groups["8-14 days"]["count"] += 1
                            expiration_groups["8-14 days"]["value"] += value
                        elif days < 31:
                            expiration_groups["15-30 days"]["count"] += 1
                            expiration_groups["15-30 days"]["value"] += value
                        else:
                            expiration_groups["> 30 days"]["count"] += 1
                            expiration_groups["> 30 days"]["value"] += value
                    
                    analysis_text += "\n### Upcoming Expirations\n"
                    analysis_text += "| Time Frame | Loans | Value | % of Total Value |\n"
                    analysis_text += "|------------|-------|-------|------------------|\n"
                    
                    for timeframe, data in expiration_groups.items():
                        value_pct = (data["value"] / total_debt_value) * 100 if total_debt_value > 0 else 0
                        analysis_text += f"| {timeframe} | {data['count']} | ${data['value']:,.2f} | {value_pct:.1f}% |\n"
                    
                    # Add rollover risk assessment
                    seven_day_value = expiration_groups["< 1 day"]["value"] + expiration_groups["1-3 days"]["value"] + expiration_groups["4-7 days"]["value"]
                    seven_day_pct = (seven_day_value / total_debt_value) * 100 if total_debt_value > 0 else 0
                    
                    analysis_text += "\n**Rollover Risk Assessment**:\n"
                    if seven_day_pct > 40:
                        analysis_text += f"**High Rollover Risk**: {seven_day_pct:.1f}% of total loan value expires within 7 days. This concentration of near-term expirations could create significant liquidity pressure if borrowers face challenges rolling over their positions.\n"
                    elif seven_day_pct > 20:
                        analysis_text += f"**Moderate Rollover Risk**: {seven_day_pct:.1f}% of total loan value expires within 7 days. This represents a notable but manageable concentration of near-term expirations.\n"
                    else:
                        analysis_text += f"**Low Rollover Risk**: Only {seven_day_pct:.1f}% of total loan value expires within 7 days. Expirations are well-distributed, reducing pressure from concentrated rollover needs.\n"
        
        # Add loans table
        analysis_text += "\n## Active Loans\n"
        analysis_text += "| Protocol | Collateral Value | Debt Value | Health | Interest | Expiration | Days Left |\n"
        analysis_text += "|----------|-----------------|------------|--------|----------|------------|----------|\n"
        
        for loan in page_data['items']:
            protocol = loan.get('protocol', 'Unknown')
            collateral_value = float(loan.get('collateralValue', 0))
            debt_value = float(loan.get('debtValue', 0))
            health = float(loan.get('health', 0))
            interest_value = float(loan.get('interestValue', 0))
            expiration = int(loan.get('expiration', 0))
            
            # Format expiration date
            expiration_date = datetime.fromtimestamp(expiration).strftime('%Y-%m-%d') if expiration > 0 else 'Unknown'
            
            # Calculate days remaining
            days_remaining = loan.get('days_remaining', 'N/A')
            if days_remaining != 'N/A':
                days_remaining = f"{days_remaining:.1f}"
            
            analysis_text += f"| {protocol} | ${collateral_value:,.2f} | ${debt_value:,.2f} | {health:.2f} | ${interest_value:,.2f} | {expiration_date} | {days_remaining} |\n"
        
        # Add pagination information
        if page_data['total_pages'] > 1:
            analysis_text += format_pagination_info(page_data)
        
        # Add disclaimer
        analysis_text += "\n## Disclaimer\n"
        analysis_text += "This analysis is for informational purposes only and not financial advice. "
        analysis_text += "DeFi lending involves significant risks including potential loss of principal. "
        analysis_text += "Always do your own research before participating in any lending protocol."
        
        # Handle chunking if needed
        should_chunk = get_config_value("enable_chunking", chunk_output)
        results = []
        
        if should_chunk:
            chunks = chunk_text(analysis_text)
            for chunk in chunks:
                results.append(types.TextContent(type="text", text=chunk))
        else:
            results.append(types.TextContent(type="text", text=analysis_text))
        
        return results
        
    except Exception as e:
        logger.error(f"Error in active_loans_analysis: {e}")
        logger.error(traceback.format_exc())
        
        error_text = "# Error Retrieving Active Loans Data\n\n"
        error_text += f"An error occurred while retrieving active loans data: {str(e)}\n"
        
        # Get the full token database to help user
        all_tokens = await get_asset_database()
        
        if all_tokens:
            error_text += "\n## Available Tokens\n"
            error_text += "Here are some popular tokens you can try:\n\n"
            
            error_text += "| Name | Ticker | Policy ID | Hex Name | Full Unit |\n"
            error_text += "|------|--------|-----------|----------|----------|\n"
            
            # Show top 15 tokens by quantity
            sorted_tokens = sorted(all_tokens, key=lambda x: x.get('quantity', 0) or 0, reverse=True)
            for token in sorted_tokens[:15]:
                name = token.get('registry_name') or token.get('name_small') or 'Unknown'
                ticker = token.get('registry_ticker', '')
                policy = token.get('policy', '')
                hex_name = token.get('name', '')
                full_unit = f"{policy}{hex_name}"
                
                error_text += f"| {name} | {ticker} | {policy[:8]}... | {hex_name} | {full_unit[:16]}... |\n"
        
        return [types.TextContent(type="text", text=error_text)]


@mcp.tool(name="market_stats", description="Get aggregated market stats, including 24h DEX volume and total active addresses onchain.")
@format_results_decorator
async def market_stats(quote:str,ignore_cache:bool=False):
    """
    Get aggregated market stats, including 24h DEX volume and total active addresses onchain. Active addresses are addresses that have either sent or received any transactions within the last 24 hours. Multiple addresses with the same stake key will be counted as one address.
        
    Args:
        quote: str (# Example: quote=ADA Quote currency to use (ADA, USD, EUR, ETH, BTC).        Default  is ADA.)
        ignore_cache: boolean (False by default)
    """
    return await taptools_api.get_market_stats(quote= quote,ignore_cache= ignore_cache)


@mcp.tool(name="nft_sale_history", description="Get a specific asset's sale history")
@format_results_decorator
async def nft_sale_history(policy:str, name:str ="",ignore_cache:bool=False):
    """
        Get a specific asset's sale history.
        Args:-
        policy :- string (required) (Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728
        The policy ID for the collection)

        name :- string (Example: name=ClayNation3725 The name of a specific nft to get stats for)
        ignore_cache: boolean (False by default)
    """
    return await taptools_api.get_nft_sale_history(policy = policy, name= name,ignore_cache= ignore_cache)

@mcp.tool(name="nft_stats", description="Get high-level stats on a certain nft asset.")
@format_results_decorator
async def nft_stats(policy: str, name: str= "",ignore_cache:bool=False):
    """
    Get high-level stats on a certain nft asset.

    Args:
        policy: str (# Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.)
        name: str (# Example: name=ClayNation3725 The name of a specific nft to get stats for.)
        ignore_cache: boolean (False by default)
    """
    return await taptools_api.get_nft_stats(policy=policy, name=name,ignore_cache= ignore_cache)


@mcp.tool(name="nft_traits", description="Get a specific nft's traits and trait prices.")

async def nft_traits(policy: str, name: str, prices: str = "1", ignore_cache:bool=False):
    """
    Get a specific nft's traits and trait prices.

    Args:
        policy: str (# Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.)
        name: str (# Example: name=ClayNation3725 The name of a specific nft to get stats for.)
        prices: str (# Example: prices=0 Include trait prices or not, Options are 0, 1. Default is 1.)
        ignore_cache: boolean (False by default)
    """
    try:
        results =await taptools_api.get_nft_traits(policy=policy, name=name, prices=prices,ignore_cache= ignore_cache)
        data = results[0]
        err = results[1]
        component = results[2]
        
        if err: return f"Could not get {component} {err}"
        result_text= f"rank:{data.get('rank','')}\n"

        result_text += format_results(data.get("traits",[]))

        return result_text
    except Exception as e:
        logger.error(f"Error in nft_traits tool {e}")
        return "Could not fetch nft traits error ocuured"


@mcp.tool(name="Collection_assets", description="Get all nfts from a collection with sorting and filtering options.")
@format_results_decorator
async def collection_assets(policy: str, sortBy: str = "price", order: str = "asc", search: str ="", onSale: str = "0",  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 100, ignore_cache:bool=False,traits:dict = {}):
    """
    Get all nfts from a collection with the ability to sort by price/rank and filter to specific traits.

    Args:
        policy: str (# Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.)
        sortBy: str (# Example: sortBy=price What should the results be sorted by. Options: price, rank.)
        order: str (# Example: order=asc Which direction should the results be sorted. Options: asc, desc.)
        search: str required (# Example: search=ClayNation3725 Search for a certain nft's name.)
        onSale: str (# Example: onSale=1 Return only nfts that are on sale. Options: 0, 1.)
        page: int (# Example: page=1 Pagination support.)
        perPage: int (# Example: perPage=100 Number of items per page. Max is 100.)
        traits: dict optional (# Trait filters as additional keyword arguments.)
        ignore_cache: boolean (False by default)
    """
    return await taptools_api.get_collection_assets(
        policy=policy,
        sortBy=sortBy,
        order=order,
        search=search,
        onSale=onSale,
        page=page,
        perPage=perPage,
        ignore_cache= ignore_cache,
        **traits
    )


@mcp.tool(name="holder_distribution", description="Get the distribution of nfts within a collection by bucketing into number of nfts held groups.")
@format_results_decorator
async def nft_holder_distribution(policy: str,ignore_cache:bool=False):
    """
    Get the distribution of nfts within a collection by bucketing into number of nfts held groups.

    Args:
        policy: str (# Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.)
        ignore_cache: boolean (False by default)
    """
    return await taptools_api.get_nft_holder_distribution(policy=policy, ignore_cache= ignore_cache)

@mcp.tool(name="nft_top_holders", description="Get the top nft holders in a collection, including listed and staked assets.")
@format_results_decorator
async def nft_top_holders(policy: str,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 10, excludeExchanges: int = 0, ignore_cache: bool = False):
    """
        Get the top holders for a particular nft collection.
        This includes owners with listed or staked nfts.

        Args:
            policy: str
                Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728
                The policy ID for the collection.

            page: int, optional
                Example: page=1
                This endpoint supports pagination. Default is 1.

            perPage: int, optional
                Example: perPage=10
                Specify how many items to return per page. Max is 100, default is 10.

            excludeExchanges: int, optional
                Example: excludeExchanges=1
                Whether or not to exclude marketplace addresses (0, 1)

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
        """
    return await taptools_api.get_nft_top_holders(
        policy=policy,
        page=page,
        perPage=perPage,
        excludeExchanges=excludeExchanges,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_holder_trend", description="Get daily trended holder count for a collection, including listed and staked assets.")
@format_results_decorator
async def nft_holder_trend(policy: str, timeframe: str = "30d", ignore_cache: bool = False):
    """
    Get holders trended by day for a particular nft collection. This includes owners with listed or staked nfts.

    Args
    policy
        str Example: policy=40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728 The policy ID for the collection.

    timeframe
        str, optional Example: timeframe=30d The time interval. Options: 7d, 30d, 90d, 180d, 1y, all. Defaults to 30d.

    ignore_cache
        bool, optional Default: False Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_holder_trend(
        policy=policy,
        timeframe=timeframe,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_collection_info", description="Get basic info about an nft collection such as name, socials, and logo.")
@format_results_decorator
async def nft_collection_info(policy: str, ignore_cache: bool = False):
    """
    Get basic information about a collection like name, socials, and logo.

    Args
    policy
        str Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e The policy ID for the collection.

    ignore_cache
        bool, optional Default: False Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_collection_info(
        policy=policy,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="number_of_active_listing", description="Get the number of active listings and total supply for a particular nft collection.")
@format_results_decorator
async def number_of_active_listing(
    policy: str,
    ignore_cache: bool = False
):
    """
    Get the number of active listings and total supply for a particular nft collection.

    Args:
        policy: str
            The policy ID for the collection.
        
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.

    Returns:
        dict:
            A dictionary containing:
            - 'listings': Number of active listings in the collection.
            - 'supply': Total number of nfts minted/supplied in the collection.
    """
    # Call the function to get the active listings info
    return await taptools_api.get_number_of_active_listing(
        policy=policy,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_listings_depth", description="Get cumulative listings count by price levels from floor upward.")
@format_results_decorator
async def nft_listings_depth(policy: str, items: int = 500, ignore_cache: bool = False):
    """
        Get cumulative amount of listings at each price point, starting at the floor and moving upwards.

        Args:
            policy: str
                Example: policy=1fcf4baf8e7465504e115dcea4db6da1f7bed335f2a672e44ec3f94e
                The policy ID for the collection.

            items: int, optional
                Example: items=600
                Specify how many items to return. Maximum is 1000, default is 500.

            ignore_cache: bool, optional
                Default: False
                Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_listings_depth(
        policy=policy,
        items=items,
        ignore_cache=ignore_cache
    )




@mcp.tool(name="nft_active_listings", description="Get a list of active nft listings with pagination and sorting.")
@format_results_decorator
async def nft_active_listings(
    policy: str,
    sortBy: str = "price",
    order: str = "asc",
     page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ),
    perPage: int = 100,
    ignore_cache: bool = False
):
    """
    Get a list of active listings with supporting information. This endpoint supports pagination and sorting by price and time listed.

    Args:
        policy: str
            The policy ID for the collection.

        sortBy: str, optional
            Example: sortBy=price
            What should the results be sorted by. Options are price, time. Default is price.

        order: str, optional
            Example: order=asc
            Which direction should the results be sorted. Options are asc, desc. Default is asc

        page: int, optional
            Example: page=1
            This endpoint supports pagination. Default page is 1.

        perPage: int, optional
            Example: perPage=100
            Specify how many items to return per page. Maximum is 100, default is 100.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_active_listings(
        policy=policy,
        sortBy=sortBy,
        order=order,
        page=page,
        perPage=perPage,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_listings_trended", description="Get trended number of listings and floor price over time.")
@format_results_decorator
async def nft_listings_trended(
    policy: str,
    interval: str,
    numIntervals: int = None,
    ignore_cache: bool = False
):
    """
    Get trended number of listings and floor price for a particular nft collection.

    Args:
        policy: str
            The policy ID for the collection.

        interval: str
            Example: interval=1d
            The time interval. Options are 3m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 3d, 1w, 1M.

        numIntervals: int, optional
            Example: numIntervals=180
            The number of intervals to return. Leave blank for full history.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_listings_trended(
        policy=policy,
        interval=interval,
        numIntervals=numIntervals,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_floor_price_ohlcv", description="Get OHLCV (open, high, low, close, volume) data for nft floor price.")
@format_results_decorator
async def nft_floor_price_ohlcv(
    policy: str,
    interval: str,
    numIntervals: int = None,
    quote: str = "ada",
    ignore_cache: bool = False
):
    """
    Get OHLCV (open, high, low, close, volume) of floor price for a particular nft collection.

    Args:
        policy: str
            The policy ID for the collection.

        interval: str
            Example: interval=1d
            The time interval. Options: 3m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 3d, 1w, 1M.

        numIntervals: int, optional
            Example: numIntervals=180
            The number of intervals to return.

        quote: str, optional
            Example: quote=ada
            Quote currency to use, e.g. USD, EUR, ADA. Defaults to ADA.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_floor_price_ohlcv(
        policy=policy,
        interval=interval,
        numIntervals=numIntervals,
        quote=quote,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_collection_stats", description="Get basic statistics about an nft collection.")
@format_results_decorator
async def nft_collection_stats(policy: str, ignore_cache: bool = False):
    """
    Get basic information about a collection like floor price, volume, and supply.

    Args:
        policy: str
            The policy ID for the collection.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_collection_stats(
        policy=policy,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="nft_collection_stats_extended", description="Get extended statistics about an nft collection with percent changes.")
@format_results_decorator
async def nft_collection_stats_extended(policy: str, timeframe: str = "24h", ignore_cache: bool = False):
    """
    Get extended statistics about a collection, including floor price, volume, supply, and percent changes over a timeframe.

    Args:
        policy: str
            The policy ID for the collection.

        timeframe: str, optional
            The time interval for percent changes. Options are 24h, 7d, 30d. Default is 24h.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_collection_stats_extended(
        policy=policy,
        timeframe=timeframe,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="nft_collection_trades", description="Get individual trades for a particular collection.")
@format_results_decorator
async def nft_collection_trades(policy: str,min_amount: int, from_timestamp: int,timeframe: str = "30d", sort_by: str = "time", order: str = "desc", 
                                   page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), per_page: int = 100, 
                                ignore_cache: bool = False):
    """
    Get individual trades for a particular collection or for the entire nft market.

    Args:
        policy: str, optional
            The policy ID for the collection.

        timeframe: str, optional
            The time interval for trades. Options are 1h, 4h, 24h, 7d, 30d, 90d, 180d, 1y, all. Default is 30d.

        sort_by: str, optional
            The attribute to sort by. Options are 'amount' or 'time'. Default is 'time'.

        order: str, optional
            The order for sorting. Options are 'asc' or 'desc'. Default is 'desc'.

        min_amount: int, optional
            The minimum amount of trade value to filter trades.

        from_timestamp: int, optional
            Filter trades after a specific UNIX timestamp.

        page: int, optional
            The page number for pagination. Default is 1.

        per_page: int, optional
            The number of items per page. Maximum is 100, default is 100.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    
    return await taptools_api.get_nft_collection_trades(
        policy=policy,
        timeframe=timeframe,
        sort_by=sort_by,
        order=order,
        min_amount=min_amount,
        from_timestamp=from_timestamp,
        page=page,
        per_page=per_page,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="nft_collection_trades_stats", description="Get trading stats for a particular collection.")
@format_results_decorator
async def nft_collection_trades_stats(policy: str, timeframe: str = "24h", ignore_cache: bool = False):
    """
    Get trading statistics like volume, sales, and buyers/sellers for a collection.

    Args:
        policy: str
            The policy ID for the collection.

        timeframe: str, optional
            The time interval for the stats. Options are 1h, 4h, 24h, 7d, 30d, all. Default is 24h.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_collection_trades_stats(
        policy=policy,
        timeframe=timeframe,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="nft_collection_traits_price", description="Get prices of traits within a collection.")

async def nft_collection_traits_price(policy: str, name: str = None, ignore_cache: bool = False):
    """
    Get a list of traits and their floor prices within a collection.

    Args:
        policy: str
            The policy ID for the collection.

        name: str, optional
            The name of a specific nft to get trait prices for.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
     """
    try:
        results =  await taptools_api.get_nft_collection_traits_price(
            policy=policy,
            ignore_cache=ignore_cache
        )
        data,err,component = results
        if err: return f"return could not get  {component} error:{err}"
        result_text = ""
        for key, val in data.items():
            result_text+=f"{key}\n"+ format_results(val)+"\n"
        return result_text
    except Exception as e:
        logger.error(f"Error occurred in nft_collection_traits_price: {str(e)}\n{traceback.format_exc()}")

        return "Error fetching the service"



def extract_tool_schema(func):
    """Extract tool schema from function signature and docstring"""
    try:
        # Get function signature
        sig = inspect.signature(func)
        
        # Get type hints
        type_hints = get_type_hints(func)
        
        # Parse docstring for parameter descriptions
        docstring = inspect.getdoc(func) or ""
        
        # Extract parameter info
        properties = {}
        required = []
        
        for param_name, param in sig.parameters.items():
            if param_name in ['self', 'cls']:
                continue
                
            param_info = {
                "type": "string"  # default type
            }
            
            # Get type from annotations
            if param_name in type_hints:
                param_type = type_hints[param_name]
                if param_type == int:
                    param_info["type"] = "integer"
                elif param_type == float:
                    param_info["type"] = "number"
                elif param_type == bool:
                    param_info["type"] = "boolean"
                elif param_type == str:
                    param_info["type"] = "string"
                elif get_origin(param_type) == list:
                    param_info["type"] = "array"
                elif get_origin(param_type) == dict:
                    param_info["type"] = "object"
            
            # Extract description from docstring
            if docstring:
                # Look for parameter in docstring
                lines = docstring.split('\n')
                for line in lines:
                    if param_name in line and ':' in line:
                        # Extract description after parameter name
                        parts = line.split(':', 1)
                        if len(parts) > 1:
                            desc = parts[1].strip()
                            if desc:
                                param_info["description"] = desc
                        break
            
            # Set default value if available
            if param.default != inspect.Parameter.empty:
                param_info["default"] = param.default
            else:
                # Parameter is required if no default
                required.append(param_name)
            
            properties[param_name] = param_info
        
        return {
            "type": "object",
            "properties": properties,
            "required": required
        }
    except Exception as e:
        logger.error(f"Error extracting schema for {func.__name__}: {e}")
        return {
            "type": "object",
            "properties": {},
            "required": []
        }

def get_all_mcp_tools():
    """Get all registered MCP tools with their schemas"""
    tools = []
    
    try:
        # Get current module to scan for decorated functions
        current_module = inspect.getmodule(inspect.currentframe())
        current_globals = globals()
        
        # Scan all functions in the current module for MCP tools
        for name, obj in inspect.getmembers(current_module, inspect.isfunction):
            # Check all async functions as potential MCP tools
            if inspect.iscoroutinefunction(obj) and not name.startswith('_'):
                try:
                    # Get the original function if it's wrapped
                    original_func = getattr(obj, '__wrapped__', obj)

                    # Extract tool information
                    tool_info = {
                        "name": name,
                        "description": inspect.getdoc(original_func) or f"Cardano MCP tool: {name}",
                        "input_schema": extract_tool_schema(original_func)
                    }

                    tools.append(tool_info)
                    logger.info(f"✅ Found MCP tool: {name}")

                except Exception as e:
                    logger.error(f"Error processing tool {name}: {e}")
        
        # Also check globals for any other tool functions
        for name, obj in current_globals.items():
            if (callable(obj) and 
                not name.startswith('_') and 
                not name in ['main', 'initialize_cache', 'configure_output_handling', 'print_available_tools'] and
                name not in [tool['name'] for tool in tools]):
                
                try:
                    # Check if it looks like a tool function
                    if (name.startswith(('get_', 'search_', 'active_', 'analyze_', 'fetch_', 'list_', 'check_', 'token_')) or
                        hasattr(obj, '__wrapped__')):
                        
                        original_func = getattr(obj, '__wrapped__', obj)
                        
                        tool_info = {
                            "name": name,
                            "description": inspect.getdoc(original_func) or f"Cardano MCP tool: {name}",
                            "input_schema": extract_tool_schema(original_func)
                        }
                        
                        tools.append(tool_info)
                        logger.info(f"✅ Found additional tool: {name}")
                        
                except Exception as e:
                    logger.error(f"Error processing additional tool {name}: {e}")
        
        logger.info(f"🎯 Successfully found {len(tools)} MCP tools")
        return tools
        
    except Exception as e:
        logger.error(f"Error getting MCP tools: {e}")
        return []

# HTTP handlers for the tools API
async def get_tools_handler(request: Request):
    """Get all available MCP tools with their schemas"""
    try:
        tools = get_all_mcp_tools()
        
        response = {
            "success": True,
            "count": len(tools),
            "tools": tools,
            "server_info": {
                "name": "Cardano FastMCP Server",
                "version": "1.0.0", 
                "transport": "FastMCP",
                "base_url": f"http://0.0.0.0:{PORT}"
            }
        }
        
        logger.info(f"📋 Returning {len(tools)} tools via /tools endpoint")
        return JSONResponse(response)
        
    except Exception as e:
        logger.error(f"Error in /tools endpoint: {e}")
        return JSONResponse({
            "success": False,
            "error": str(e),
            "tools": [],
            "count": 0
        }, status_code=500)

async def call_tool_handler(request: Request):
    """Execute a specific MCP tool"""
    try:
        tool_name = request.path_params.get('tool_name')
        request_data = await request.json()
        
        logger.info(f"🛠️ Calling tool: {tool_name} with data: {request_data}")
        
        # Get the current module and look for the tool function
        current_module = inspect.getmodule(inspect.currentframe())
        current_globals = globals()
        
        # Find the tool function
        tool_func = None
        
        # First check module members
        for name, obj in inspect.getmembers(current_module, inspect.isfunction):
            if name == tool_name:
                tool_func = obj
                break
        
        # Then check globals
        if not tool_func and tool_name in current_globals:
            potential_func = current_globals[tool_name]
            if callable(potential_func):
                tool_func = potential_func
        
        if not tool_func:
            logger.error(f"❌ Tool '{tool_name}' not found")
            available_tools = [name for name, obj in inspect.getmembers(current_module, inspect.isfunction) 
                             if name.startswith(('get_', 'search_', 'active_', 'analyze_', 'fetch_', 'list_', 'check_', 'token_'))]
            
            return JSONResponse({
                "success": False,
                "error": f"Tool '{tool_name}' not found",
                "available_tools": available_tools[:10]  # Limit to avoid too much data
            }, status_code=404)
        
        # Execute the tool
        try:
            logger.info(f"🚀 Executing {tool_name}...")
            
            # Call the tool with provided arguments
            if inspect.iscoroutinefunction(tool_func):
                result = await tool_func(**request_data)
            else:
                result = tool_func(**request_data)
            
            logger.info(f"✅ Tool {tool_name} executed successfully")
            
            return JSONResponse({
                "success": True,
                "tool": tool_name,
                "result": result,
                "args": request_data
            })
            
        except Exception as execution_error:
            logger.error(f"❌ Tool execution failed: {execution_error}")
            return JSONResponse({
                "success": False,
                "error": f"Tool execution failed: {str(execution_error)}",
                "tool": tool_name,
                "args": request_data
            }, status_code=500)
            
    except Exception as e:
        logger.error(f"❌ Error in call_tool_handler: {e}")
        return JSONResponse({
            "success": False,
            "error": str(e)
        }, status_code=500)

# Create additional routes for the tools API
tools_routes = [
    Route("/tools", get_tools_handler, methods=["GET"]),
    Route("/tools/{tool_name}/call", call_tool_handler, methods=["POST"]),
]

# Create a Starlette app for the tools API
tools_app = Starlette(routes=tools_routes)



@mcp.tool(name="nft_collection_metadata_rarity", description="Get the rarity of metadata attributes within a collection.")
async def nft_collection_metadata_rarity(policy: str, ignore_cache: bool = False):
    """
    Get metadata attributes and their rarity within a collection.

    Args:
        policy: str
            The policy ID for the collection.

        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    try:
        results =  await taptools_api.get_nft_collection_metadata_rarity(
            policy=policy,
            ignore_cache=ignore_cache
        )
        data,err,component = results
        if err: return f"return could not get  {component} error:{err}"
        result_text = ""
        for key, val in data.items():
            result_text+=f"{key}\n"+ format_results(val)+"\n"
        return result_text
    except Exception as e:
        logger.error(f"Error occurred in nft_collection_metadata_rarity: {str(e)}\n{traceback.format_exc()}")

        return "Error fetching the service"

async def print_available_tools():
    """Debug function to print all available tools"""
    try:
        tools = get_all_mcp_tools()
        logger.info(f"\n🔧 Available MCP Tools ({len(tools)}):")
        for i, tool in enumerate(tools, 1):
            desc = tool.get('description', 'No description')
            # Truncate long descriptions
            if len(desc) > 100:
                desc = desc[:100] + "..."
            logger.info(f"  {i:2d}. {tool['name']}: {desc}")
        logger.info("")
        
        # Also log the endpoints
        logger.info("🌐 Additional HTTP Endpoints:")
        logger.info(f"  GET  http://0.0.0.0:{PORT}/tools - Get all tools")
        logger.info(f"  POST http://0.0.0.0:{PORT}/tools/{{tool_name}}/call - Execute tool")
        logger.info("")
        
    except Exception as e:
        logger.error(f"Error printing available tools: {e}")

async def display_tools_simple():
    """Simple function to display just tool names and numbers"""
    try:
        tools = get_all_mcp_tools()
        print(f"Available Tools ({len(tools)}):")
        for i, tool in enumerate(tools, 1):
            print(f"{i}. {tool['name']}")
    except Exception as e:
        print(f"❌ Error displaying tools: {e}")




@mcp.tool(name="nft_rarity_rank", description="Get rank of nft's rarity within a collection.")
@format_results_decorator
async def nft_rarity_rank(policy: str, name: str, ignore_cache: bool = False):
    """
    Get rank of nft's rarity within a collection.

    Args:
        policy: str
            The policy ID for the collection.
        name: str
            The name of the nft.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_rarity_rank(
        policy=policy,
        name=name,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_volume_trended", description="Get trended volume and number of sales for a particular nft collection.")
@format_results_decorator
async def nft_volume_trended(policy: str, interval: str, numIntervals: int = None, ignore_cache: bool = False):
    """
    Get trended volume and number of sales for a particular nft collection.

    Args:
        policy: str
            The policy ID for the collection.
        interval: str
            The time interval. Options are 3m, 5m, 15m, 30m, 1h, 2h, 4h, 12h, 1d, 3d, 1w, 1M.
        numIntervals: int, optional
            The number of intervals to return. Leave blank for full history.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_volume_trended(
        policy=policy,
        interval=interval,
        numIntervals=numIntervals,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_market_stats", description="Get high-level market stats across the entire nft market.")
@format_results_decorator
async def nft_market_stats(timeframe: str = "24h", ignore_cache: bool = False):
    """
    Get high-level market stats across the entire nft market.

    Args:
        timeframe: str, optional
            Time interval (e.g., 1h, 4h, 24h, 7d, 30d, all). Default is 24h.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_market_stats(
        timeframe=timeframe,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_market_stats_extended", description="Get high-level nft market stats with percentage changes.")
@format_results_decorator
async def nft_market_stats_extended(timeframe: str = "24h", ignore_cache: bool = False):
    """
    Get high-level market stats across the entire nft market with percentage changes.

    Args:
        timeframe: str, optional
            Time interval (e.g., 1h, 4h, 24h, 7d, 30d, all). Default is 24h.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_market_stats_extended(
        timeframe=timeframe,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_market_volume_trended", description="Get trended volume for entire nft market.")
@format_results_decorator
async def nft_market_volume_trended(timeframe: str = "30d", ignore_cache: bool = False):
    """
    Get trended volume for entire nft market.

    Args:
        timeframe: str, optional
            Time interval (e.g., 7d, 30d, 90d, 180d, 1y, all). Default is 30d.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_market_volume_trended(
        timeframe=timeframe,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_marketplace_stats", description="Get high-level stats for a specific nft marketplace.")
@format_results_decorator
async def nft_marketplace_stats(marketplace: str , lastDay: int  = 0, ignore_cache: bool = False,
                                timeframe:    str = "7d"):
    """
    Get high-level nft marketplace stats.

    Args:
        timeframe: str, 
            Time interval (e.g., 24h, 7d, 30d, 90d, 180d, all). Default is 7d.
        marketplace: str,
            Filter data to a specific marketplace (e.g., jpg.store).
        lastDay: int,
            If set to 0 or 1, filters to data between yesterday and today 00:00 UTC.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_marketplace_stats(
        timeframe=timeframe,
        marketplace=marketplace,
        lastDay=lastDay,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_top_rankings", description="Get top NFT rankings based on total market cap, 24-hour volume, or 24-hour top price gainers/losers.")
@format_results_decorator
async def nft_top_rankings(ranking: str, items: int = 25, ignore_cache: bool = False):
    """
    Get top NFT rankings based on total market cap, 24-hour volume, or 24-hour top price gainers/losers.

    Args:
        ranking: str, required
            Criteria to rank NFT collections based on. Options are marketCap, volume, gainers, losers.
        items: int, optional
            Specify how many items to return. Maximum is 100, default is 25.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_top_rankings(
        ranking=ranking,
        items=items,
        ignore_cache=ignore_cache
    )
@mcp.tool(name="nft_top_volume_collections", description="Get top NFT collections by trading volume over a specified timeframe.")
@format_results_decorator
async def nft_top_volume_collections(timeframe: str = "24h",  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 10, ignore_cache: bool = False):
    """
    Get top NFT collections by trading volume over a specified timeframe.

    Args:
        timeframe: str, optional
            The timeframe for volume aggregation. Options are: 1h, 4h, 24h, 7d, 30d, all. Defaults to 24h.
        page: int, optional
            Page number for pagination. Default is 1.
        perPage: int, optional
            Number of items to return per page (max: 100, default: 10).
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_top_volume_collections(
        timeframe=timeframe,
        page=page,
        perPage=perPage,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="nft_top_volume_collections_extended", description="Get top NFT collections by trading volume over a specified timeframe, including percentage change metrics.")
@format_results_decorator
async def nft_top_volume_collections_extended(timeframe: str = "24h",  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 10, ignore_cache: bool = False):
    """
    Get top NFT collections by trading volume over a specified timeframe, including percentage change metrics.

    Args:
        timeframe: str, optional
            The timeframe for volume aggregation. Options are: 1h, 4h, 24h, 7d, 30d, all. Defaults to 24h.
        page: int, optional
            Page number for pagination. Default is 1.
        perPage: int, optional
            Number of items to return per page (max: 100, default: 10).
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_nft_top_volume_collections_extended(
        timeframe=timeframe,
        page=page,
        perPage=perPage,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="token_links", description="Get a specific token's social links, if they have been provided to TapTools.")
@format_results_decorator
async def token_links(unit: str, ignore_cache: bool = False):
    """
    Get a specific token's social links, if they have been provided to TapTools.

    Args:
        unit: str, required
            Token unit (policy + hex name).
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_token_links(
        unit=unit,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="token_market_cap", description="Get a specific token's supply and market cap information.")
@format_results_decorator
async def token_market_cap(unit: str, ignore_cache: bool = False):
    """
    Get a specific token's supply and market cap information.

    Args:
        unit: str, required
            Token unit (policy + hex name).
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_token_market_cap(
        unit=unit,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="token_price_ohlcv", description="Get a specific token's trended (open, high, low, close, volume) price data.")
@format_results_decorator
async def token_price_ohlcv(interval: str, unit: str = None, onchainID: str = None, numIntervals: int = None, ignore_cache: bool = False):
    """
    Get a specific token's trended (open, high, low, close, volume) price data. You can either pass a token unit 
    to get aggregated data across all liquidity pools, or an onchainID for a specific pair.
    
    Args:
        interval: str, required
            The time interval. Options are `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `12h`, `1d`, `3d`, `1w`, `1M`.
        unit: str, optional
            Token unit (policy + hex name).
        onchainID: str, optional
            Pair onchain ID to get ohlc data for.
        numIntervals: int, optional
            The number of intervals to return, e.g. if you want 180 days of data in 1d intervals, then pass `180` here.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_token_price_ohlcv(
        interval=interval,
        unit=unit,
        onchainID=onchainID,
        numIntervals=numIntervals,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="token_liquidity_pools", description="Get a specific token's active liquidity pools.")
@format_results_decorator
async def token_liquidity_pools(unit: str = None, onchainID: str = None, adaOnly: int = None, ignore_cache: bool = False):
    """
    Get a specific token's active liquidity pools. Can search for all token pools using unit or 
    can search for specific pool with onchainID.
    
    Args:
        unit: str, optional
            Token unit (policy + hex name).
        onchainID: str, optional
            Liquidity pool onchainID.
        adaOnly: int, optional
            Return only ADA pools or all pools (0, 1).
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_token_liquidity_pools(
        unit=unit,
        onchainID=onchainID,
        adaOnly=adaOnly,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="token_prices", description="Get prices for a list of tokens (max 100).")
@format_results_decorator
async def token_prices(tokens: list[str], ignore_cache: bool = False):
    """
    Get an object with token units (policy + hex name) as keys and price as values for a list of policies 
    and hex names. These prices are aggregated across all supported DEXs. Max batch size is 100 tokens.
    
    Args:
        tokens: list[str], required
            List of policy + hex names of tokens. Maximum of 100 tokens per request.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_token_prices(
        tokens=tokens,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="token_price_percent_change", description="Get a specific token's price percent change over various timeframes.")
@format_results_decorator
async def token_price_percent_change(unit: str, timeframes: str = None, ignore_cache: bool = False):
    """
    Get a specific token's price percent change over various timeframes. Timeframe options include 
    [5m, 1h, 4h, 6h, 24h, 7d, 30d, 60d, 90d]. All timeframes are returned by default.
    
    Args:
        unit: str, required
            Token unit (policy + hex name).
        timeframes: str, optional
            List of timeframes (comma-separated).
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_token_price_percent_change(
        unit=unit,
        timeframes=timeframes,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="quote_price", description="Get current ADA quote price (e.g., ADA/USD).")
@format_results_decorator
async def quote_price(quote: str = "USD", ignore_cache: bool = False):
    """
    Get current quote price (e.g, current ADA/USD price). This only returns the price of ADA against 
    the specified quote currency.
    
    Args:
        quote: str, optional
            Quote currency to use (USD, EUR, ETH, BTC). Default is USD.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_quote_price(
        quote=quote,
        ignore_cache=ignore_cache
    )
    

@mcp.tool(name="available_quote_currencies", description="Get all currently available quote currencies.")
@format_results_decorator
async def available_quote_currencies(ignore_cache: bool = False):
    """
    Get all currently available quote currencies.
    
    Args:
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_available_quote_currencies(
        ignore_cache=ignore_cache
    )

@mcp.tool(name="top_liquidity_tokens", description="Get tokens ranked by their DEX liquidity.")
@format_results_decorator
async def top_liquidity_tokens( page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 10, ignore_cache: bool = False):
    """
    Get tokens ranked by their DEX liquidity. This includes both AMM and order book liquidity.
    
    Args:
        page: int, optional
            This endpoint supports pagination. Default page is 1.
        perPage: int, optional
            Specify how many items to return per page. Maximum is 100, default is 10.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_top_liquidity_tokens(
        page=page,
        perPage=perPage,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="top_market_cap_tokens", description="Get tokens with top market cap in descending order.")
@format_results_decorator
async def top_market_cap_tokens(type: str = "mcap",  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 20, ignore_cache: bool = False):
    """
    Get tokens with top market cap in a descending order. This endpoint excludes 
    deprecated tokens (e.g. MELD V1 since there was a token migration to MELD V2).
    
    Args:
        type: str, optional
            Sort tokens by circulating market cap or fully diluted value. Options ["mcap", "fdv"].
        page: int, optional
            This endpoint supports pagination. Default page is 1.
        perPage: int, optional
            Specify how many items to return per page. Maximum is 100, default is 20.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_top_market_cap_tokens(
        type=type,
        page=page,
        perPage=perPage,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="top_volume_tokens", description="Get tokens with top volume for a given timeframe.")
@format_results_decorator
async def top_volume_tokens(timeframe: str = "24h",  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 20, ignore_cache: bool = False):
    """
    Get tokens with top volume for a given timeframe.
    
    Args:
        timeframe: str, optional
            Specify a timeframe in which to aggregate the data by. 
            Options are ["1h", "4h", "12h", "24h", "7d", "30d", "180d", "1y", "all"]. 
            Default is 24h.
        page: int, optional
            This endpoint supports pagination. Default page is 1.
        perPage: int, optional
            Specify how many items to return per page. Maximum is 100, default is 20.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_top_volume_tokens(
        timeframe=timeframe,
        page=page,
        perPage=perPage,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="token_trades", description="Get token trades across the entire DEX market.")
@format_results_decorator
async def token_trades(timeframe: str = "30d", sortBy: str = "amount", order: str = "desc", 
                      unit: str = None, minAmount: int = None, from_timestamp: int = None,
                       page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 10, ignore_cache: bool = False):
    """
    Get token trades across the entire DEX market.
    
    Args:
        timeframe: str, optional
            The time interval. Options are ["1h", "4h", "24h", "7d", "30d", "90d", "180d", "1y", "all"]. 
            Defaults to 30d.
        sortBy: str, optional
            What should the results be sorted by. Options are ["amount", "time"]. 
            Default is amount. Filters to only ADA trades if set to amount.
        order: str, optional
            Which direction should the results be sorted. Options are ["asc", "desc"]. 
            Default is desc.
        unit: str, optional
            Optionally filter to a specific token by specifying a token unit (policy + hex name).
        minAmount: int, optional
            Filter to only trades of a certain ADA amount.
        from_timestamp: int, optional
            Filter trades using a UNIX timestamp, will only return trades after this timestamp.
        page: int, optional
            This endpoint supports pagination. Default page is 1.
        perPage: int, optional
            Specify how many items to return per page. Maximum is 100, default is 10.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_token_trades(
        timeframe=timeframe,
        sortBy=sortBy,
        order=order,
        unit=unit,
        minAmount=minAmount,
        from_timestamp=from_timestamp,
        page=page,
        perPage=perPage,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="trading_stats", description="Get aggregated trading stats for a particular token.")
@format_results_decorator
async def trading_stats(unit: str, timeframe: str = "24h", ignore_cache: bool = False):
    """
    Get aggregated trading stats for a particular token.
    
    Args:
        unit: str, required
            Token unit (policy + hex name)
        timeframe: str, optional
            Specify a timeframe in which to aggregate the data by. 
            Options are ["15m", "1h", "4h", "12h", "24h", "7d", "30d", "90d", "180d", "1y", "all"]. 
            Default is 24h.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_trading_stats(
        unit=unit,
        timeframe=timeframe,
        ignore_cache=ignore_cache
    )


@mcp.tool(name="asset_supply", description="Get onchain supply for a token.")
@format_results_decorator
async def asset_supply(unit: str, ignore_cache: bool = False):
    """
    Get onchain supply for a token.
    
    Args:
        unit: str, required
            Example: unit=8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441
            Token unit (policy + hex name) to filter by
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_asset_supply(
        unit=unit,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="address_info", description="Get address payment credential and stake address, along with its current aggregate lovelace and multi asset balance.")
async def address_info(address: str = None, paymentCred: str = None, ignore_cache: bool = False):
    """
    Get address payment credential and stake address, along with its current aggregate lovelace and multi asset balance.
    Either `address` or `paymentCred` can be provided, but one must be provided.
    
    Args:
        address: str, optional
            Example: address=addr1q9j5jqhqak5nmqphdqt4cj9kq0gppa49afyznggw03hjzhwxr0exydkt78th5wwrjphxh0h6rrgghzwxse6q3pdf9sxqkg2mmq
            Address to query for
        paymentCred: str, optional
            Example: paymentCred=654902e0eda93d803768175c48b603d010f6a5ea4829a10e7c6f215d
            Payment credential to query for
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    try:
        data, err, component = await taptools_api.get_address_info(
            address=address,
            paymentCred=paymentCred,
            ignore_cache=ignore_cache
        )
        
        if err:
            return f"Could not get {component} err- {err}"
        
        # Format complex objects for better display
        assets = data.get("assets", [])
        if "assets" in data:
            del data["assets"]
        result_text= format_results(data)
        result_text ="\nassets\n" + format_results(assets)

        return result_text
        
    except Exception as e:
        logger.error(f"Error ocuured in address_info tool err - {str(e)}")
        logger.error(traceback.format_exc())
        return f"Error ocuured in address_info tool err - {str(e)}"

@mcp.tool(name="address_utxos", description="Get current UTxOs at an address/payment credential.")
async def address_utxos(address: str = None, paymentCred: str = None,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 100, ignore_cache: bool = False):
    """
    Get current UTxOs at an address/payment credential.
    Either `address` or `paymentCred` can be provided, but one must be provided.
    
    Args:
        address: str, optional
            Address to query for
        paymentCred: str, optional
            Payment credential to query for
        page: int, optional
            This endpoint supports pagination. Default page is `1`.
        perPage: int, optional
            Specify how many items to return per page. Maximum is `100`, default is `100`.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    try:
        data, err, component = await taptools_api.get_address_utxos(
            address=address,
            paymentCred=paymentCred,
            page=page,
            perPage=perPage,
            ignore_cache=ignore_cache
        )
        
        if err:
            return f"Error in {component} err: {err}"
        
        result_text = ""
        for item in data:
            assets = item.get("assets",[])
            del item["assets"]
            result_text+="\nutxos\n" + format_results(item)+"\nassets\n" + format_results(assets)
        return result_text
    except Exception as e:
        logger.error(f"Error ocurred in addres_utxos tool err:{str(e)}")
        logger.error(traceback.format_exc())
        return f"Error ocurred in addres_utxos tool err:{str(e)}"

@mcp.tool(name="transaction_utxos", description="Get UTxOs from a specific transaction.")
async def transaction_utxos(hash: str, ignore_cache: bool = False):
    """
    Get UTxOs from a specific transaction.
    
    Args:
        hash: str, required
            Example: hash=8be33680ec04da1cc98868699c5462fbbf6975529fb6371669fa735d2972d69b
            Transaction hash
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    try:
        data, err, component = await taptools_api.get_transaction_utxos(
            hash=hash,
            ignore_cache=ignore_cache
        )
        
        if err:
            return f"Error in {component} err: {err}"
        
        result_text = f"hash: {data.get('hash','')}\n"
        inputs= data.get("inputs",[])
        outputs=  data.get("outputs",[])

        result_text+="inputs:-"
        for item in inputs:
            assets = item.get("assets",[])
            del item["assets"]
            result_text+="\nutxos\n" + format_results(item)+"\nassets\n" + format_results(assets)

        result_text+="outputs:-"
        for item in outputs:
            assets = item.get("assets",[])
            del item["assets"]
            result_text+="\nutxos\n" + format_results(item)+"\nassets\n" + format_results(assets)
        return result_text
    except Exception as e:
        logger.error(f"Error ocurred in transaction_utxos tool err:{str(e)}")
        logger.error(traceback.format_exc())
        return  f"Error ocurred in transaction utxos tool err:{str(e)}"


@mcp.tool(name="get_portfolio_positions", description="Get wallet's current portfolio positions with supporting market data.")

async def get_portfolio_positions(address: str, ignore_cache: bool = False):
    """
    Get wallet's current portfolio positions with supporting market data.
    
    Args:
        address: str, required
            Address to query for
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    try:
        results =  await taptools_api.get_portfolio_positions(
            address=address,
            ignore_cache=ignore_cache
        )
        data,err,component = results

        if err: return f"Could not get {component} error: {err}"
        positionsFt = data.get("positionsFt",[])
        positionsLp= data.get("positionsLp",[])
        positionsNft = data.get("positionsNft",[])

        if "positionsFt" in data: del data["positionsFt"]
        if "positionsLp" in data: del data["positionsLp"]
        if "positionsNft" in data: del data["positionsNft"]

        result_text = format_results(data)+"\n"
        result_text += "positionsFt"+ format_results(positionsFt) +"\n"
        result_text += "positionsLp"+ format_results(positionsLp) +"\n"
        result_text += "positionsNft"+ format_results(positionsNft) +"\n"

        return result_text
    except Exception as e:
        logger.error(f"Error ocurred in get_portfolio_positions tool err:{str(e)}")
        logger.error(traceback.format_exc())
        return  f"Error ocurred in get_portfolio_positions tool err:{str(e)}"

@mcp.tool(name="get_token_trade_history", description="Get the token trade history for a particular wallet.")
@format_results_decorator
async def get_token_trade_history(address: str, unit: str = None,  page: int = Field(
        ...,
        description="Page number for pagination. Increase page number to get more results example page=1,2,3.....",
        ge=1
    ), perPage: int = 100, ignore_cache: bool = False):
    """
    Get the token trade history for a particular wallet.
    
    Args:
        address: str, required
            Address to query for
        unit: str, optional
            Token unit (policy + hex name) to filter by
        page: int, optional
            This endpoint supports pagination. Default page is 1.
        perPage: int, optional
            Specify how many items to return per page. Maximum is 100, default is 100.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_token_trade_history(
        address=address,
        unit=unit,
        page=page,
        perPage=perPage,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="get_portfolio_trended_value", description="Get historical trended value of an address in 4hr intervals.")
@format_results_decorator
async def get_portfolio_trended_value(address: str, timeframe: str = "30d", quote: str = "ADA", ignore_cache: bool = False):
    """
    Get historical trended value of an address in 4hr intervals.
    
    Args:
        address: str, required
            Address to query for
        timeframe: str, optional
            The time interval. Options are 24h, 7d, 30d, 90d, 180d, 1y, all. Defaults to 30d.
        quote: str, optional
            Quote currency to use (ADA, USD, EUR, ETH, BTC). Default is ADA.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    return await taptools_api.get_portfolio_trended_value(
        address=address,
        timeframe=timeframe,
        quote=quote,
        ignore_cache=ignore_cache
    )

@mcp.tool(name="get_token_by_id", description="Returns details of a given token by its address.")
async def get_token_by_id(id: str, ignore_cache: bool = False):
    """
    Returns details of a given token by its address.
    
    Args:
        id: str, required
            Token ID
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    data, err, component = await taptools_api.get_token_by_id(
        id=id,
        ignore_cache=ignore_cache
    )
    if err:
        return f"Could not get {component}, err: {err}"
    
    main_key = "asset"
    result_text = f"{main_key}\n"
    result_text += format_results(data.get(main_key, {}))
    return result_text


@mcp.tool(name="get_block", description="Returns a specific block using either the number of the block or its timestamp.")
async def get_block(number: int = None, timestamp: int = None, ignore_cache: bool = False):
    """
    Returns a specific block using either the number of the block or its timestamp.
    
    Args:
        number: int, optional
            Block number
        timestamp: int, optional
            Block timestamp
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    data, err, component = await taptools_api.get_block(
        number=number,
        timestamp=timestamp,
        ignore_cache=ignore_cache
    )
    if err:
        return f"Could not get {component}, err: {err}"
    
    main_key = "block"
    result_text = f"{main_key}\n"
    result_text += format_results(data.get(main_key, {}))
    return result_text


@mcp.tool(name="get_events", description="List of events occurred in a range of blocks.")
async def get_events(fromBlock: int, toBlock: int, limit: int = 1000, ignore_cache: bool = False):
    """
    List of events occurred in a range of blocks.
    
    Args:
        fromBlock: int, required
            Block number to start filtering at (inclusive).
        toBlock: int, required
            Block number to end filtering at (inclusive).
        limit: int, optional
            Limit results to a maximum count, Defaults to 1000, With a maximum of 1000.
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    data, err, component = await taptools_api.get_events(
        fromBlock=fromBlock,
        toBlock=toBlock,
        limit=limit,
        ignore_cache=ignore_cache
    )
    if err:
        return f"Could not get {component}, err: {err}"
    
    main_key = "events"
    result_text = f"{main_key}\n"
    result_text += format_results(data.get(main_key, []))
    return result_text


@mcp.tool(name="get_dex", description="Return details of a given DEX by its factory address or alternative id.")
async def get_dex(id: str, ignore_cache: bool = False):
    """
    Return details of a given DEX by its factory address or alternative id.
    
    Args:
        id: str, required
            Exchange id
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    data, err, component = await taptools_api.get_dex(
        id=id,
        ignore_cache=ignore_cache
    )
    if err:
        return f"Could not get {component}, err: {err}"
    
    main_key = "exchange"
    result_text = f"{main_key}\n"
    result_text += format_results(data.get(main_key, {}))
    return result_text


@mcp.tool(name="get_latest_block", description="Returns the latest block processed in the blockchain/DEX.")
async def get_latest_block(ignore_cache: bool = False):
    """
    Returns the latest block processed in the blockchain/DEX.
    
    Args:
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    data, err, component = await taptools_api.get_latest_block(
        ignore_cache=ignore_cache
    )
    if err:
        return f"Could not get {component}, err: {err}"
    
    main_key = "block"
    result_text = f"{main_key}\n"
    result_text += format_results(data.get(main_key, {}))
    return result_text


@mcp.tool(name="get_pair_by_id", description="Returns pair details (aka pool) by its address.")
async def get_pair_by_id(id: str, ignore_cache: bool = False):
    """
    Returns pair details (aka pool) by its address.
    
    Args:
        id: str, required
            Pair ID
        ignore_cache: bool, optional
            Default: False
            Whether to bypass cache and fetch fresh data.
    """
    data, err, component = await taptools_api.get_pair_by_id(
        id=id,
        ignore_cache=ignore_cache
    )
    if err:
        return f"Could not get {component}, err: {err}"
    
    main_key = "pair"
    result_text = f"{main_key}\n"
    result_text += format_results(data.get(main_key, {}))
    return result_text

@mcp.tool(
    name="get_token_meta",
    description="Get token metadata including policy ID, hex name, and full_unit identifier. Use the returned 'full_unit' field (policy+hex concatenated) for all other token-related APIs like active_loans_analysis, token_holders_analysis, etc. Always use this tool first to get the correct full_unit identifier before calling other token APIs."
)
@format_results_decorator
@cache_results(table="token_meta_info", service_name="token_meta_info", ttl_hours = -1)
@rate_limit(5)
async def get_token_meta(name: str, ignore_cache: bool = False):
    """
    Get comprehensive token metadata including the full_unit identifier needed for other APIs.
    
    Takes a human-readable token name and returns policy ID, hex name, and most importantly
    the 'full_unit' field which is the concatenated policy+hex identifier required by
    other token analysis APIs.
    
    Args:
        name (str): The human-readable token name/ticker, e.g., 'SNEK', 'ADA', 'MIN'
        ignore_cache (bool): Whether to ignore cache, default is False
        
    Returns:
        dict: Token metadata including:
            - policy: Policy ID (64 hex characters)
            - name: Hex-encoded token name  
            - full_unit: Policy+hex concatenated (USE THIS for other APIs)
            - registry_name: Human-readable name
            - registry_ticker: Token ticker/symbol
            - quantity: Total supply
            - Other metadata fields
            
    Usage:
        1. Call this function with token name: get_token_meta("SNEK")
        2. Extract the 'full_unit' field from response
        3. Use that full_unit value for active_loans_analysis, token_holders_analysis, etc.
        
    Example:
        meta = get_token_meta("SNEK")
        full_unit = meta['full_unit']  # "279c909f...534e454b" 
        loans = active_loans_analysis(full_unit)  # Use full_unit here
    """
    try:
        
        if not await ensure_cache_connection():
            logger.warning("Cache not available for asset database")
            raise Exception("Cache not available for asset database")
        search_text = name
        result = supabase.from_("cexplorer_assets").select("*").or_(
                    
                    f"name_small.ilike.%{search_text}%,"
                    f"name.ilike.%{search_text}%,"
                    f"registry_name.ilike.%{search_text}%,"
                    f"registry_ticker.ilike.%{search_text}%"
                    ).limit(1).execute()
        data= result.data

        if not data:
            raise RuntimeError("Cant find token in database")
        if isinstance(data, dict): data = [data]
        if len(data) !=0 and data is not None:
            for item in data:
                # print(item)
                item["full_unit"] = f"{item['policy']}{item['name']}"


        return [data, None]
    except Exception as e:
        logger.error(f"Could not get token meta info for token {name} {e}", exc_info = True)
        return [None, f"Could not get token meta info for token {name} {e}\n check if token name is correct and try again. web search can be used to find the correct token name"]


@mcp.tool(
    name="dexes_address_lookup",
    description="Look up DeFi platform information for Cardano addresses. Returns details like platform type, description, and categorization for known DeFi/DEX addresses."
)
async def dexes_address_lookup(addresses: List[str], ignore_cache: bool = False):
    """
    Look up DeFi platform information for a list of Cardano addresses.

    This tool queries a database of known DeFi protocol addresses and returns
    information about which platforms they belong to, their types, and descriptions.

    Args:
        addresses (List[str]): List of Cardano addresses to lookup. Can include payment addresses,
                              stake addresses, or contract addresses. Examples:
                              - addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha
                              - stake1u9f9v0z5zzlldgx58n8tklphu8mf7h4jvp2j2gddluemnssjfnkzz
        ignore_cache (bool): Whether to ignore cache and fetch fresh data, default is False

    Returns:
        dict: Results containing:
            - success: Boolean indicating if the request was successful
            - total_queried: Number of addresses queried
            - total_found: Number of addresses found in database
            - total_not_found: Number of addresses not found
            - results: Object containing:
                - found: Array of found addresses with their DeFi platform details
                - not_found: Array of addresses not found in the database

    Example response:
        {
            "success": true,
            "total_queried": 2,
            "total_found": 1,
            "total_not_found": 1,
            "results": {
                "found": [
                    {
                        "address": "addr1...",
                        "entries": [
                            {
                                "id": 1,
                                "defi_platform": "Uniswap",
                                "type": "DEX",
                                "address": "addr1...",
                                "description": "Main trading contract"
                            }
                        ]
                    }
                ],
                "not_found": [
                    {
                        "address": "addr2...",
                        "message": "Address not found in DEXES database"
                    }
                ]
            }
        }
    """
    try:
        # Validate inputs
        if not addresses or not isinstance(addresses, list):
            return {
                "success": False,
                "error": "Invalid input: addresses must be a non-empty list"
            }

        if len(addresses) > 100:
            return {
                "success": False,
                "error": "Too many addresses: maximum 100 addresses allowed per request"
            }

        # Make request to the API
        api_url = f"{os.getenv('SYNCAI_BASE_URL', 'http://localhost:4000')}/api/cardano/dexes/addresses/lookup"
        api_key = os.getenv('SYNCAI_KEY')

        if not api_key:
            return {
                "success": False,
                "error": "API key not configured for DEXES lookup service"
            }

        headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key
        }

        payload = {
            "addresses": addresses
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(api_url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    result = await response.json()
                    return result
                else:
                    error_text = await response.text()
                    return {
                        "success": False,
                        "error": f"API request failed with status {response.status}: {error_text}"
                    }

    except aiohttp.ClientTimeout:
        return {
            "success": False,
            "error": "Request timeout: DEXES lookup service is taking too long to respond"
        }
    except Exception as e:
        logger.error(f"Error in dexes_address_lookup: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": f"Unexpected error occurred: {str(e)}"
        }




# Initialize cache on startup

async def initialize_cache():
    """Initialize the cache connection on server startup"""
    global redis_cache, supabase,cache_available
    logger.info("Initializing Redis cache on server startup")
    redis_cache = RedisDb(pool=pool)

    await redis_cache.init_db()
    
    # Optional: Perform a test query to ensure connection is working
    if redis_cache and redis_cache.cache_available:
        logger.info("Redis Cache initialization successful")
    else:
        logger.warning("Redis Cache initialization failed - will attempt reconnection when needed")
    logger.info("Initializing Supabase cache on server startup")
    await init_supabase()
    
    # Optional: Perform a test query to ensure connection is working
    if cache_available:
        logger.info("Supabase Cache initialization successful")
    else:
        logger.warning("Supabase Cache initialization failed - will attempt reconnection when needed")





async def test_tools():
    # Ensure res.txt exists or create it, and clear the previous content

    file_path = "res.txt"
    with open(file_path, "w") as f:
        f.write("Testing Results:\n\n")
    
    # Open the file manually
    f = open(file_path, "a")

    try:
        # Tool 55: Get Pair by ID
        payload = {
            "id": "nikeswaporderbook.44759dc63605dbf88700b241ee451aa5b0334cf2b34094d836fbdf8642757a7a696542656520.ada",
            "ignore_cache": False
        }
        x = await get_pair_by_id(**payload)
        f.write(f"Tool 55 (Get Pair by ID) Result:\n{x}\n\n")
        logger.info("Tool 55 (Get Pair by ID) run success")
        print("\n\n")

        # Tool 54: Get Latest Block
        payload = {
            "ignore_cache": False
        }
        x = await get_latest_block(**payload)
        f.write(f"Tool 54 (Get Latest Block) Result:\n{x}\n\n")
        logger.info("Tool 54 (Get Latest Block) run success")
        print("\n\n")

        # Tool 53: Get DEX
        payload = {
            "id": "7",
            "ignore_cache": False
        }
        x = await get_dex(**payload)
        f.write(f"Tool 53 (Get DEX) Result:\n{x}\n\n")
        logger.info("Tool 53 (Get DEX) run success")
        print("\n\n")

        # Tool 52: Get Events
        payload = {
            "fromBlock": 10937538,
            "toBlock": 10937542,
            "limit": 1000,
            "ignore_cache": False
        }
        x = await get_events(**payload)
        f.write(f"Tool 52 (Get Events) Result:\n{x}\n\n")
        logger.info("Tool 52 (Get Events) run success")
        print("\n\n")

        # Tool 51: Get Block
        payload = {
            "number": 10937538,
            "ignore_cache": False
        }
        x = await get_block(**payload)
        f.write(f"Tool 51 (Get Block) Result:\n{x}\n\n")
        logger.info("Tool 51 (Get Block) run success")
        print("\n\n")

        # Tool 50: Get Token by ID
        payload = {
            "id": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "ignore_cache": False
        }
        x = await get_token_by_id(**payload)
        f.write(f"Tool 50 (Get Token by ID) Result:\n{x}\n\n")
        logger.info("Tool 50 (Get Token by ID) run success")
        print("\n\n")
        # Tool 49: Get Portfolio Trended Value
        payload = {
            "address": "stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc",
            "timeframe": "30d",
            "quote": "ADA",
            "ignore_cache": False
        }
        x = await get_portfolio_trended_value(**payload)
        f.write(f"Tool 49 (Get Portfolio Trended Value) Result:\n{x}\n\n")
        logger.info("Tool 49 (Get Portfolio Trended Value) run success")
        print("\n\n")

        # Tool 48: Get Token Trade History
        payload = {
            "address": "stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc",
            
            "page": 1,
            "perPage": 100,
            "ignore_cache": False
        }
        x = await get_token_trade_history(**payload)
        f.write(f"Tool 48 (Get Token Trade History) Result:\n{x}\n\n")
        logger.info("Tool 48 (Get Token Trade History) run success")
        print("\n\n")

        # Tool 47: Get Portfolio Positions
        payload = {
            "address": "stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc",
            "ignore_cache": False
        }
        x = await get_portfolio_positions(**payload)
        
        f.write(f"Tool 47 (Get Portfolio Positions) Result:\n{x}\n\n")
        logger.info("Tool 47 (Get Portfolio Positions) run success")
        print("\n\n")
       
        # Tool 46: Transaction_UTxOs
        payload = {
            "hash": "8be33680ec04da1cc98868699c5462fbbf6975529fb6371669fa735d2972d69b",
            "ignore_cache": False
        }
        x = await transaction_utxos(**payload)
        f.write(f"Tool 46 (Transaction_UTxOs) Result:\n{x}\n\n")
        logger.info("Tool 46 (Transaction_UTxOs) run success")
        print("\n\n")

        # Tool 45: Address_UTxOs
        payload = {
            "address": "addr1q8elqhkuvtyelgcedpup58r893awhg3l87a4rz5d5acatuj9y84nruafrmta2rewd5l46g8zxy4l49ly8kye79ddr3ksqal35g",
            
           
            "ignore_cache": False
        }
        x = await address_utxos(**payload)
        f.write(f"Tool 45 (Address_UTxOs) Result:\n{x}\n\n")
        logger.info("Tool 45 (Address_UTxOs) run success")
        print("\n\n")

        # Tool 44: Address_Info
        payload = {
            "address": "addr1q8elqhkuvtyelgcedpup58r893awhg3l87a4rz5d5acatuj9y84nruafrmta2rewd5l46g8zxy4l49ly8kye79ddr3ksqal35g",
            
            "ignore_cache": False
        }
        x = await address_info(**payload)
        f.write(f"Tool 44 (Address_Info) Result:\n{x}\n\n")
        logger.info("Tool 44 (Address_Info) run success")
        print("\n\n")

        # Tool 43: Asset_Supply
        payload = {
            "unit": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "ignore_cache": False
        }
        x = await asset_supply(**payload)
        f.write(f"Tool 43 (Asset_Supply) Result:\n{x}\n\n")
        logger.info("Tool 43 (Asset_Supply) run success")
        print("\n\n")

       
        # Tool 42: Trading_Stats
        payload = {
            "unit": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "timeframe": "24h",
            "ignore_cache": False
        }
        x = await trading_stats(**payload)
        f.write(f"Tool 42 (Trading_Stats) Result:\n{x}\n\n")
        logger.info("Tool 42 (Trading_Stats) run success")
        print("\n\n")

        # Tool 41: Token_Trades
        payload = {
            "timeframe": "30d",
            "sortBy": "amount",
            "order": "desc",
            "unit": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "minAmount": 1000,
            "from_timestamp": None,
            "page": 1,
            "perPage": 10,
            "ignore_cache": False
        }
        x = await token_trades(**payload)
        f.write(f"Tool 41 (Token_Trades) Result:\n{x}\n\n")
        logger.info("Tool 41 (Token_Trades) run success")
        print("\n\n")

        # Tool 40: Top_Volume_Tokens
        payload = {
            "timeframe": "24h",
            "page": 1,
            "perPage": 20,
            "ignore_cache": False
        }
        x = await top_volume_tokens(**payload)
        f.write(f"Tool 40 (Top_Volume_Tokens) Result:\n{x}\n\n")
        logger.info("Tool 40 (Top_Volume_Tokens) run success")
        print("\n\n")

        # Tool 39: Top_Market_Cap_Tokens
        payload = {
            "type": "mcap",
            "page": 1,
            "perPage": 20,
            "ignore_cache": False
        }
        x = await top_market_cap_tokens(**payload)
        f.write(f"Tool 39 (Top_Market_Cap_Tokens) Result:\n{x}\n\n")
        logger.info("Tool 39 (Top_Market_Cap_Tokens) run success")
        print("\n\n")

        # Tool 38: Top_Liquidity_Tokens
        payload = {
            "page": 1,
            "perPage": 10,
            "ignore_cache": False
        }
        x = await top_liquidity_tokens(**payload)
        f.write(f"Tool 38 (Top_Liquidity_Tokens) Result:\n{x}\n\n")
        logger.info("Tool 38 (Top_Liquidity_Tokens) run success")
        print("\n\n")

        # Tool 37: Available_Quote_Currencies
        payload = {
            "ignore_cache": False
        }
        x = await available_quote_currencies(**payload)
        f.write(f"Tool 37 (Available_Quote_Currencies) Result:\n{x}\n\n")
        logger.info("Tool 37 (Available_Quote_Currencies) run success")
        print("\n\n")

        # Tool 36: Quote_Price
        payload = {
            "quote": "USD",
            "ignore_cache": False
        }
        x = await quote_price(**payload)
        f.write(f"Tool 36 (Quote_Price) Result:\n{x}\n\n")
        logger.info("Tool 36 (Quote_Price) run success")
        print("\n\n")

        # Tool 35: Token_Price_Percent_Change
        payload = {
            "unit": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "timeframes": "1h,24h,7d",
            "ignore_cache": False
        }
        x = await token_price_percent_change(**payload)
        f.write(f"Tool 35 (Token_Price_Percent_Change) Result:\n{x}\n\n")
        logger.info("Tool 35 (Token_Price_Percent_Change) run success")
        print("\n\n")

        # Tool 34: Token_Prices
        payload = {
            "tokens": [
                "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
                "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c64d494e"
            ],
            "ignore_cache": False
        }
        x = await token_prices(**payload)
        f.write(f"Tool 34 (Token_Prices) Result:\n{x}\n\n")
        logger.info("Tool 34 (Token_Prices) run success")
        print("\n\n")

        # Tool 33: Token_Liquidity_Pools
        payload = {
            "unit": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "adaOnly": 1,
            "ignore_cache": False
        }
        x = await token_liquidity_pools(**payload)
        f.write(f"Tool 33 (Token_Liquidity_Pools) Result:\n{x}\n\n")
        logger.info("Tool 33 (Token_Liquidity_Pools) run success")
        print("\n\n")

        # Tool 32: Token_Price_OHLCV
        payload = {
            "interval": "1d",
            "unit": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "numIntervals": 30,
            "ignore_cache": False
        }
        x = await token_price_ohlcv(**payload)
        f.write(f"Tool 32 (Token_Price_OHLCV) Result:\n{x}\n\n")
        logger.info("Tool 32 (Token_Price_OHLCV) run success")
        print("\n\n")
        
        # Tool 31: Token_Market_Cap
        payload = {
            "unit": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "ignore_cache": False
        }
        x = await token_market_cap(**payload)
        f.write(f"Tool 31 (Token_Market_Cap) Result:\n{x}\n\n")
        logger.info("Tool 31 (Token_Market_Cap) run success")
        print("\n\n")

        # Tool 30: Token_Links
        payload = {
            "unit": "8fef2d34078659493ce161a6c7fba4b56afefa8535296a5743f6958741414441",
            "ignore_cache": False
        }
        x = await token_links(**payload)
        f.write(f"Tool 30 (Token_Links) Result:\n{x}\n\n")
        logger.info("Tool 30 (Token_Links) run success")
        print("\n\n")

        # Tool 29: NFT_Top_Volume_Collections_Extended
        payload = {
            "timeframe": "24h",
            "page": 1,
            "perPage": 10,
            "ignore_cache": False
        }
        x = await nft_top_volume_collections_extended(**payload)
        f.write(f"Tool 29 (NFT_Top_Volume_Collections_Extended) Result:\n{x}\n\n")
        logger.info("Tool 29 (NFT_Top_Volume_Collections_Extended) run success")
        print("\n\n")

        # Tool 28: NFT_Top_Volume_Collections
        payload = {
            "timeframe": "24h",
            "page": 1,
            "perPage": 10,
            "ignore_cache": False
        }
        x = await nft_top_volume_collections(**payload)
        f.write(f"Tool 28 (NFT_Top_Volume_Collections) Result:\n{x}\n\n")
        logger.info("Tool 28 (NFT_Top_Volume_Collections) run success")
        print("\n\n")

        # Tool 27: NFT_Top_Rankings
        payload = {
            "ranking": "marketCap",
            "items": 25,
            "ignore_cache": False
        }
        x = await nft_top_rankings(**payload)
        f.write(f"Tool 27 (NFT_Top_Rankings) Result:\n{x}\n\n")
        logger.info("Tool 27 (NFT_Top_Rankings) run success")
        print("\n\n")

        
        # Tool 26: nft_Marketplace_Stats
        payload = {
            "marketplace": "jpg.store",
            "timeframe": "7d"
        }
        x = await nft_marketplace_stats(**payload)
        f.write(f"Tool 26 (nft_Marketplace_Stats) Result:\n{x}\n\n")
        logger.info("Tool 26 (nft_Marketplace_Stats) run success")
        print("\n\n")

        # Tool 25: nft_Market_Volume_Trended
        payload = {
            "timeframe": "30d"
        }
        x = await nft_market_volume_trended(**payload)
        f.write(f"Tool 25 (nft_Market_Volume_Trended) Result:\n{x}\n\n")
        logger.info("Tool 25 (nft_Market_Volume_Trended) run success")
        print("\n\n")

        # Tool 24: nft_Market_Stats_Extended
        payload = {
            "timeframe": "24h"
        }
        x = await nft_market_stats_extended(**payload)
        f.write(f"Tool 24 (nft_Market_Stats_Extended) Result:\n{x}\n\n")
        logger.info("Tool 24 (nft_Market_Stats_Extended) run success")
        print("\n\n")

        # Tool 23: nft_Market_Stats
        payload = {
            "timeframe": "24h"
        }
        x = await nft_market_stats(**payload)
        f.write(f"Tool 23 (nft_Market_Stats) Result:\n{x}\n\n")
        logger.info("Tool 23 (nft_Market_Stats) run success")
        print("\n\n")

        # Tool 22: nft_Volume_Trended
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "interval": "1d"
        }
        x = await nft_volume_trended(**payload)
        f.write(f"Tool 22 (nft_Volume_Trended) Result:\n{x}\n\n")
        logger.info("Tool 22 (nft_Volume_Trended) run success")
        print("\n\n")

        # Tool 21: nft_Rarity_Rank
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "name": "ClayNation3725"
        }
        x = await nft_rarity_rank(**payload)
        f.write(f"Tool 21 (nft_Rarity_Rank) Result:\n{x}\n\n")
        logger.info("Tool 21 (nft_Rarity_Rank) run success")
        print("\n\n")

        
        # Tool 20: nft Collection Metadata Rarity
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728"
        }
        x = await nft_collection_metadata_rarity(**payload)
        f.write(f"Tool 20 (nft_Collection_Metadata_Rarity) Result:\n{x}\n\n")
        logger.info("Tool 20 (nft_Collection_Metadata_Rarity) run success")
        print("\n\n")

        # Tool 19: nft Collection Traits Price
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "name": "ClayNation3725"
        }
        x = await nft_collection_traits_price(**payload)
        f.write(f"Tool 19 (nft_Collection_Traits_Price) Result:\n{x}\n\n")
        logger.info("Tool 19 (nft_Collection_Traits_Price) run success")
        print("\n\n")

        # Tool 18: nft Collection Trades Stats
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "timeframe": "24h"
        }
        x = await nft_collection_trades_stats(**payload)
        f.write(f"Tool 18 (nft_Collection_Trades_Stats) Result:\n{x}\n\n")
        logger.info("Tool 18 (nft_Collection_Trades_Stats) run success")
        print("\n\n")

        # Tool 17: nft Trades
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "timeframe": "30d",
            "sort_by": "time",
            "order": "desc",
            "from_timestamp":1704759422,
            "min_amount": 1000,
            "page": 1,
            "per_page": 100
        }
        x = await nft_collection_trades(**payload)
        f.write(f"Tool 17 (nft_Trades) Result:\n{x}\n\n")
        logger.info("Tool 17 (nft_Trades) run success")
        print("\n\n")

        # Tool 16: nft Collection Stats Extended
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728"
        }
        x = await nft_collection_stats_extended(**payload)
        f.write(f"Tool 16 (nft_Collection_Stats_Extended) Result:\n{x}\n\n")
        logger.info("Tool 16 (nft_Collection_Stats_Extended) run success")
        print("\n\n")
        
        
        # Tool 15: nft Collection Stats
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728"
        }
        x = await nft_collection_stats(**payload)
        f.write(f"Tool 15 (nft_Collection_Stats) Result:\n{x}\n\n")
        logger.info("Tool 15 (nft_Collection_Stats) run success")
        print("\n\n")

        # Tool 14: nft Floor Price OHLCV
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "interval": "1d",
            "numIntervals": 30,
            "quote": "ada"
        }
        x = await nft_floor_price_ohlcv(**payload)
        f.write(f"Tool 14 (nft_Floor_Price_OHLCV) Result:\n{x}\n\n")
        logger.info("Tool 14 (nft_Floor_Price_OHLCV) run success")
        print("\n\n")

        # Tool 13: nft Listings Trended
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "interval": "1d",
            "numIntervals": 30
        }
        x = await nft_listings_trended(**payload)
        f.write(f"Tool 13 (nft_Listings_Trended) Result:\n{x}\n\n")
        logger.info("Tool 13 (nft_Listings_Trended) run success")
        print("\n\n")
        
        # Tool 12: nft Active Listings
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728"
        }
        x = await nft_active_listings(**payload)
        f.write(f"Tool 12 (nft_Active_Listings) Result:\n{x}\n\n")
        logger.info("Tool 12 (nft_Active_Listings) run success")
        print("\n\n")

        # Tool 11: nft Listings Depth
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "items": 500
        }
        x = await nft_listings_depth(**payload)
        f.write(f"Tool 11 (nft_Listings_Depth) Result:\n{x}\n\n")
        logger.info("Tool 11 (nft_Listings_Depth) run success")
        print("\n\n")

        # Tool 10: Number of Active Listings
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728"
        }
        x = await number_of_active_listing(**payload)
        f.write(f"Tool 10 (Number_of_Active_Listing) Result:\n{x}\n\n")
        logger.info("Tool 10 (Number_of_Active_Listing) run success")
        print("\n\n")



        # Tool 9: nft Collection Info
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728"
        }
        x = await nft_collection_info(**payload)
        f.write(f"Tool 9 (nft_Collection_Info) Result:\n{x}\n\n")
        logger.info("Tool 9 (nft_Collection_Info) run success")
        print("\n\n")

        # Tool 8: nft Holder Trend
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "timeframe": "30d"
        }
        x = await nft_holder_trend(**payload)
        f.write(f"Tool 8 (nft_Holder_Trend) Result:\n{x}\n\n")
        logger.info("Tool 8 (nft_Holder_Trend) run success")
        print("\n\n")

        # Tool 7: nft Top Holders
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "page": 1,
            "perPage": 10
        }
        x = await nft_top_holders(**payload)
        f.write(f"Tool 7 (nft_Top_Holders) Result:\n{x}\n\n")
        logger.info("Tool 7 (nft_Top_Holders) run success")
        print("\n\n")

        # Tool 6: Holder Distribution
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728"
        }
        x = await nft_holder_distribution(**payload)
        f.write(f"Tool 6 (Holder_Distribution) Result:\n{x}\n\n")
        logger.info("Tool 6 (Holder_Distribution) run success")
        print("\n\n")

        # Tool 5: Collection Assets
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "sortBy": "price",
            "order": "asc",
            "onSale": "1",
            "page": 1,
            "perPage": 100
        }
        x = await collection_assets(**payload)
        f.write(f"Tool 5 (Collection_Assets) Result:\n{x}\n\n")
        logger.info("Tool 5 (Collection_Assets) run success")
        print("\n\n")

        # Tool 4: nft Traits
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "name": "ClayNation3725",
            "prices": "1",
        }
        x = await nft_traits(**payload)
        f.write(f"Tool 4 (nft_Traits) Result:\n{x}\n\n")
        logger.info("Tool 4 (nft_Traits) run success")
        print("\n\n")

        # Tool 3: nft Stats
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "name": "ClayNation3725"
        }
        x = await nft_stats(**payload)
        f.write(f"Tool 3 (nft_Stats) Result:\n{x}\n\n")
        logger.info("Tool 3 (nft_Stats) run success")
        print("\n\n")

        # Tool 2: nft Sale History
        payload = {
            "policy": "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728",
            "name": "ClayNation3725"
        }
        x = await nft_sale_history(**payload)
        f.write(f"Tool 2 (nft_Sale_History) Result:\n{x}\n\n")
        logger.info("Tool 2 (nft_Sale_History) run success")
        print("\n\n")

        # Tool 1: Market Stats
        payload = {"quote": "ADA"}
        x = await market_stats(**payload)
        f.write(f"Tool 1 (Market_Stats) Result:\n{x}\n\n")
        logger.info("Tool 1 (Market_Stats) run success")
        print("\n\n")

    finally:
        # Manually close the file
        f.close()




async def main():
    await initialize_cache()
    await print_available_tools()
    # payload = {"name": "SNEK", "ignore_cache":True}
    # print(await get_token_meta(**payload))
    print(await check_proposal_access("gov_action123"))

    # await test_tools()

if __name__ == "__main__":
    # Use initialize_cache directly as a startup handler
    # asyncio.run(main())

    mcp.start_handlers = [initialize_cache, display_tools_simple]

    asyncio.run(display_tools_simple())

    TRANSPORT = os.environ.get("TRANSPORT", "sse")
    mcp.run(
        transport=TRANSPORT,
    )