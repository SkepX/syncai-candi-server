"""
CrewAI Agent that uses custom tools to call Anthropic API with MCP
"""
import os
from typing import Dict, Any
from anthropic import Anthropic
from dotenv import load_dotenv
import logging
from crewai import Agent, Task, Crew, Process
from crewai.tools import tool

load_dotenv()

logger = logging.getLogger(__name__)

# Initialize Anthropic client
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CARDANO_MCP_URL = os.getenv("CARDANO_MCP_URL", "http://localhost:4324")
MODEL_NAME = "claude-sonnet-4-20250514"

anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


@tool("Query Cardano MCP")
def query_cardano_mcp(query: str) -> str:
    """
    Query the Cardano blockchain using MCP tools via Anthropic API.

    This tool uses Claude with access to 86+ Cardano MCP tools to answer
    questions about governance, tokens, NFTs, DeFi, and on-chain data.

    Args:
        query: Natural language question about Cardano blockchain

    Returns:
        Comprehensive answer with real blockchain data
    """
    try:
        if not anthropic_client:
            return "Error: Anthropic API client not initialized"

        logger.info(f"🔧 Tool called: query_cardano_mcp with query: {query}")
        logger.info(f"🌐 Using MCP server: {CARDANO_MCP_URL}")

        # Make API call to Claude with MCP server
        message = anthropic_client.beta.messages.create(
            model=MODEL_NAME,
            max_tokens=2500,
            messages=[
                {
                    "role": "user",
                    "content": query
                }
            ],
            mcp_servers=[
                {
                    "type": "url",
                    "url": CARDANO_MCP_URL,
                    "name": "cardano-mcp",
                    "tool_configuration": {
                        "enabled": True,
                    },
                },
            ],
            betas=["mcp-client-2025-04-04"],
        )

        # Extract the response text
        result_text = ""
        if message.content:
            for block in message.content:
                if hasattr(block, 'text'):
                    result_text += block.text

        logger.info(f"✅ MCP query successful")
        return result_text

    except Exception as e:
        logger.error(f"❌ Error in MCP query: {e}", exc_info=True)
        return f"Error querying Cardano MCP: {str(e)}"


class CardanoExpertCrew:
    """
    CrewAI crew with a Cardano expert agent that uses MCP tools
    """

    def __init__(self):
        """Initialize the Cardano Expert Crew with MCP tool"""
        self.tool = query_cardano_mcp
        self.agent = self._create_agent()
        self.crew = self._create_crew()

    def _create_agent(self) -> Agent:
        """
        Create a Cardano expert agent with access to MCP query tool

        Returns:
            Agent configured with MCP query tool
        """
        return Agent(
            role="Cardano Blockchain Expert",
            goal="Provide comprehensive analysis and insights about Cardano blockchain data using MCP tools",
            backstory="""You are an expert Cardano blockchain analyst with deep knowledge of:
            - Cardano governance (DReps, SPOs, Constitutional Committee, Proposals)
            - On-chain data analysis (accounts, addresses, transactions, UTXOs)
            - Token economics (pricing, liquidity, market cap, trading)
            - NFT markets (collections, sales, traits, rarity)
            - DeFi protocols (loans, DEXes, liquidity pools)
            - Portfolio tracking and wallet analysis

            You have access to a powerful tool that connects to 86+ specialized Cardano MCP tools
            through Claude. You always use this tool to get accurate, real-time blockchain data
            and provide data-driven insights.""",
            tools=[self.tool],
            verbose=True,
            allow_delegation=False,
            llm="anthropic/claude-sonnet-4-20250514"
        )

    def _create_crew(self) -> Crew:
        """
        Create the CrewAI crew with the Cardano expert agent

        Returns:
            Configured Crew instance
        """
        return Crew(
            agents=[self.agent],
            tasks=[],  # Tasks will be created dynamically
            process=Process.sequential,
            verbose=True
        )

    def process_query(self, query: str) -> Dict[str, Any]:
        """
        Process a user query using the Cardano expert crew

        Args:
            query: User's question or request about Cardano blockchain

        Returns:
            Dictionary containing:
                - success: Boolean indicating if query was successful
                - result: The crew's response
                - query: Original query
                - error: Error message if failed
        """
        try:
            logger.info(f"🚀 CrewAI processing query: {query}")

            # Create a task for this specific query
            task = Task(
                description=f"""Use the query_cardano_mcp tool to answer this question about Cardano:

{query}

Make sure to:
1. Use the tool to get real, up-to-date blockchain data
2. Provide specific numbers, metrics, and data points
3. Explain technical concepts clearly
4. Include relevant context and analysis""",
                expected_output="""A comprehensive answer that:
1. Directly answers the user's question with real data
2. Provides specific metrics and numbers
3. Includes relevant context
4. Is well-formatted and easy to understand""",
                agent=self.agent
            )

            # Update crew with new task
            self.crew.tasks = [task]

            # Execute the crew
            logger.info("🔄 Executing CrewAI crew...")
            result = self.crew.kickoff()

            # Extract result
            if hasattr(result, 'raw'):
                output = result.raw
            elif hasattr(result, 'result'):
                output = result.result
            else:
                output = str(result)

            logger.info("✅ CrewAI processing complete")

            return {
                "success": True,
                "result": output,
                "query": query,
                "error": None
            }

        except Exception as e:
            logger.error(f"❌ CrewAI error: {e}", exc_info=True)
            return {
                "success": False,
                "result": None,
                "query": query,
                "error": str(e)
            }


# Singleton instance
_crew_instance = None


def get_agent() -> CardanoExpertCrew:
    """
    Get or create the singleton CardanoExpertCrew instance

    Returns:
        CardanoExpertCrew instance
    """
    global _crew_instance
    if _crew_instance is None:
        _crew_instance = CardanoExpertCrew()
    return _crew_instance


def process_query(query: str) -> Dict[str, Any]:
    """
    Process a query using the Cardano expert crew

    Args:
        query: User's question about Cardano blockchain

    Returns:
        Dictionary with success status, result, and metadata
    """
    crew = get_agent()
    return crew.process_query(query)
