"""
CrewAI Crew Definition for Cardano Blockchain Analysis
Defines agents, tasks, and crew configuration
"""

from crewai import Agent, Crew, Process
from crewai.tools import tool
import os
import logging
from typing import Dict, Any
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Anthropic client for MCP integration
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CARDANO_MCP_URL = os.getenv("CARDANO_MCP_URL", "http://localhost:4324")
MODEL_NAME = "claude-sonnet-4-20250514"

anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


# ============================================================================
# Custom Tool: Cardano MCP Query
# ============================================================================

@tool("Query Cardano Blockchain via MCP")
def query_cardano_mcp(query: str) -> str:
    """
    Query the Cardano blockchain using MCP tools via Anthropic API.

    This tool uses Claude with access to 86+ Cardano MCP tools to answer
    questions about:
    - Governance (DReps, SPOs, Constitutional Committee, Proposals)
    - On-chain data (accounts, addresses, transactions, UTXOs)
    - Token economics (pricing, liquidity, market cap, trading)
    - NFT markets (collections, sales, traits, rarity)
    - DeFi protocols (loans, DEXes, liquidity pools)
    - Portfolio tracking and wallet analysis

    Args:
        query: Natural language question about Cardano blockchain

    Returns:
        Comprehensive answer with real blockchain data
    """
    try:
        if not anthropic_client:
            return "Error: Anthropic API client not initialized. Please set ANTHROPIC_API_KEY."

        logger.info(f"🔧 Tool called: query_cardano_mcp")
        logger.info(f"📝 Query: {query}")
        logger.info(f"🌐 MCP Server: {CARDANO_MCP_URL}")

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


# ============================================================================
# Cardano Research Crew
# ============================================================================

class CardanoResearchCrew:
    """
    CrewAI crew for Cardano blockchain research and analysis
    """

    def __init__(self):
        """Initialize the Cardano Research Crew"""
        self.agents = self._create_agents()
        self.crew = self._create_crew()

    def _create_agents(self) -> list[Agent]:
        """
        Create specialized agents for Cardano analysis

        Returns:
            List of configured agents
        """

        # Main Cardano Blockchain Expert
        cardano_expert = Agent(
            role="Cardano Blockchain Expert",
            goal="Provide comprehensive analysis and insights about Cardano blockchain data using MCP tools",
            backstory="""You are a world-class Cardano blockchain analyst with deep expertise in:

            - Cardano Governance: Understanding of DReps, SPOs, Constitutional Committee,
              governance proposals, and voting mechanisms
            - On-Chain Analysis: Proficient in analyzing accounts, addresses, transactions,
              UTXOs, and blockchain metrics
            - Token Economics: Expert knowledge of ADA pricing, token liquidity, market cap,
              trading volumes, and tokenomics
            - NFT Markets: Deep understanding of NFT collections, sales data, traits,
              rarity analysis, and market trends
            - DeFi Protocols: Expertise in loans, DEXes, liquidity pools, and DeFi metrics
            - Portfolio Analysis: Skilled in wallet tracking and portfolio management

            You have access to a powerful tool that connects to 86+ specialized Cardano MCP tools
            through Claude. You ALWAYS use this tool to get accurate, real-time blockchain data
            rather than relying on general knowledge.

            Your approach:
            1. Use the MCP tool to fetch current, accurate data
            2. Analyze the data with your expertise
            3. Provide clear, actionable insights
            4. Include specific numbers, metrics, and context
            5. Explain technical concepts in an accessible way
            """,
            tools=[query_cardano_mcp],
            verbose=True,
            allow_delegation=False,
            llm="anthropic/claude-sonnet-4-20250514"
        )

        return [cardano_expert]

    def _create_crew(self) -> Crew:
        """
        Create the CrewAI crew with agents

        Returns:
            Configured Crew instance
        """
        return Crew(
            agents=self.agents,
            tasks=[],  # Tasks will be created dynamically per request
            process=Process.sequential,
            verbose=True,
            memory=False,  # Disable memory for stateless operation
            cache=False    # Disable cache for fresh data
        )

    def get_crew(self) -> Crew:
        """Get the configured crew"""
        return self.crew

    def get_agent(self) -> Agent:
        """Get the main Cardano expert agent"""
        return self.agents[0]


# ============================================================================
# Singleton Instance
# ============================================================================

_crew_instance = None


def get_research_crew() -> CardanoResearchCrew:
    """
    Get or create singleton CardanoResearchCrew instance

    Returns:
        CardanoResearchCrew instance
    """
    global _crew_instance
    if _crew_instance is None:
        _crew_instance = CardanoResearchCrew()
        logger.info("✅ CardanoResearchCrew initialized")
    return _crew_instance
