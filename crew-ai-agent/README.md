# Cardano CrewAI Agent - Masumi Compatible

AI agent for Cardano blockchain analysis with decentralized payment integration via Masumi Network. This agent uses CrewAI with 86+ Cardano MCP tools to provide comprehensive blockchain insights.

## Features

- **MIP-003 Compliant**: Fully compatible with Masumi payment standard
- **Cardano Expertise**: Access to 86+ specialized Cardano MCP tools
- **Decentralized Payments**: Automated payment processing on Cardano blockchain
- **Production Ready**: FastAPI server with Railway deployment support
- **Comprehensive Analysis**: Governance, tokens, NFTs, DeFi, and on-chain data

## Quick Start

### 1. Clone and Setup

```bash
cd crew-ai-agent
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# Masumi Payment Service
PAYMENT_SERVICE_URL=http://localhost:3001/api/v1
PAYMENT_API_KEY=your_api_key

# Agent Configuration
AGENT_IDENTIFIER=your_agent_id
PAYMENT_AMOUNT=2000000
SELLER_VKEY=your_vkey

# API Keys
ANTHROPIC_API_KEY=your_anthropic_key
CARDANO_MCP_URL=https://your-mcp-server.ngrok-free.app/sse
```

### 3. Test Locally (Standalone Mode)

Test without payments:

```bash
python main.py
```

### 4. Run API Mode (With Masumi Payments)

Start the server with payment integration:

```bash
python main.py api
```

Access the API at:
- Interactive docs: http://localhost:8000/docs
- API endpoint: http://localhost:8000

## Deployment Guide

### Deploy to Railway

#### Option 1: Deploy via Railway CLI

1. Install Railway CLI:
```bash
npm i -g @railway/cli
```

2. Login to Railway:
```bash
railway login
```

3. Initialize project:
```bash
cd crew-ai-agent
railway init
```

4. Add environment variables in Railway dashboard or via CLI:
```bash
railway variables set ANTHROPIC_API_KEY="your_key"
railway variables set CARDANO_MCP_URL="https://your-mcp.ngrok-free.app/sse"
railway variables set PAYMENT_SERVICE_URL="https://your-payment.railway.app/api/v1"
railway variables set PAYMENT_API_KEY="your_payment_key"
railway variables set AGENT_IDENTIFIER="your_agent_id"
railway variables set SELLER_VKEY="your_vkey"
railway variables set PAYMENT_AMOUNT="2000000"
railway variables set NETWORK="Preprod"
```

5. Deploy:
```bash
railway up
```

#### Option 2: Deploy via GitHub

1. Push code to GitHub
2. Go to [Railway.app](https://railway.app)
3. Click "New Project" → "Deploy from GitHub repo"
4. Select your repository
5. Add environment variables in Settings → Variables
6. Railway will auto-deploy

Your agent will be available at: `https://your-project.railway.app`

### Deploy to Render

1. Go to [Render.com](https://render.com)
2. Click "New" → "Web Service"
3. Connect your GitHub repository
4. Configure:
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python main.py api`
5. Add environment variables
6. Deploy

## Masumi Integration Steps

### Step 1: Install Masumi Payment Service

Follow the [Masumi Installation Guide](https://docs.masumi.network/documentation/how-to-guides/installation)

Verify it's running:
```bash
curl http://localhost:3001/api/v1/health/
```

### Step 2: Top Up Wallet with Test ADA

1. Get your Selling Wallet address from Masumi Dashboard
2. Visit [Cardano Faucet](https://docs.cardano.org/cardano-testnets/tools/faucet/)
3. Request Test ADA (Preprod network)

### Step 3: Register Your Agent on Masumi

1. Get payment source info:
```bash
curl -X GET 'http://localhost:3001/api/v1/payment-source/' \
  -H 'accept: application/json'
```

2. Note the `walletVkey` for Preprod network

3. Register agent:
```bash
curl -X POST 'http://localhost:3001/api/v1/registry' \
  -H 'Content-Type: application/json' \
  -d '{
    "agent_name": "Cardano Blockchain Expert",
    "agent_description": "AI agent for comprehensive Cardano blockchain analysis using 86+ MCP tools",
    "service_url": "https://your-agent.railway.app",
    "input_schema": {
      "fields": [
        {
          "name": "query",
          "type": "text",
          "description": "Question about Cardano blockchain",
          "required": true
        }
      ]
    },
    "payment_amount": 2000000,
    "payment_unit": "lovelace"
  }'
```

4. Track registration in admin dashboard

5. Get your agent identifier:
```bash
curl -X GET 'http://localhost:3001/api/v1/registry' \
  -H 'accept: application/json'
```

6. Create API key:
```bash
curl -X GET 'http://localhost:3001/api/v1/api-key/' \
  -H 'accept: application/json'
```

7. Update your `.env` file with:
   - `AGENT_IDENTIFIER`
   - `PAYMENT_API_KEY`
   - `SELLER_VKEY`

### Step 4: Test Your Monetized Agent

1. Start a job:
```bash
curl -X POST "https://your-agent.railway.app/start_job" \
  -H "Content-Type: application/json" \
  -d '{
    "identifier_from_purchaser": "a1b2c3d4e5f6",
    "input_data": {
      "query": "What are the top 5 DReps by voting power on Cardano?"
    }
  }'
```

2. Note the `job_id` from response

3. Make payment (from purchaser):
```bash
curl -X POST 'http://localhost:3001/api/v1/purchase' \
  -H 'Content-Type: application/json' \
  -H 'token: purchaser_api_key' \
  -d '{
    "agent_identifier": "your_agent_identifier"
  }'
```

4. Check job status:
```bash
curl -X GET "https://your-agent.railway.app/status?job_id=your_job_id"
```

## API Endpoints

### MIP-003 Standard Endpoints

- **GET /input_schema** - Returns agent input requirements
- **GET /availability** - Checks if service is operational
- **POST /start_job** - Starts a new paid job
- **GET /status?job_id=<id>** - Checks job status
- **POST /provide_input** - Provides additional input (not supported)

### Helper Endpoints

- **GET /** - API information
- **GET /health** - Health check
- **GET /docs** - Interactive API documentation

## Environment Variables Reference

| Variable | Description | Example |
|----------|-------------|---------|
| `PAYMENT_SERVICE_URL` | Masumi payment service URL | `http://localhost:3001/api/v1` |
| `PAYMENT_API_KEY` | API key from payment service | `pk_xxx...` |
| `AGENT_IDENTIFIER` | Your agent's unique ID | `agent_xxx...` |
| `PAYMENT_AMOUNT` | Price in lovelace | `2000000` (10 ADA) |
| `PAYMENT_UNIT` | Payment unit | `lovelace` |
| `SELLER_VKEY` | Selling wallet verification key | `vkey_xxx...` |
| `NETWORK` | Cardano network | `Preprod` or `Mainnet` |
| `ANTHROPIC_API_KEY` | Anthropic/Claude API key | `sk-ant-xxx...` |
| `CARDANO_MCP_URL` | Public MCP server URL | `https://xxx.ngrok-free.app/sse` |
| `HOST` | Server host | `0.0.0.0` |
| `PORT` | Server port | `8000` |

## Agent Capabilities

Your agent can answer questions about:

### Governance
- DReps ranking and voting power
- SPO (Stake Pool Operator) data
- Constitutional Committee members
- Governance proposals and voting

### On-Chain Data
- Account balances and UTXOs
- Transaction history
- Address analysis
- Blockchain metrics

### Token Economics
- ADA price and market cap
- Token liquidity and trading volume
- Token holder distribution
- Market trends

### NFT Markets
- NFT collections and sales
- Trait analysis and rarity
- Floor prices and volume
- Collection rankings

### DeFi Protocols
- Lending protocols and loans
- DEX trading pairs
- Liquidity pools
- DeFi metrics

## Production Considerations

### Database Storage

Replace in-memory storage with PostgreSQL:

```python
# Install PostgreSQL
pip install sqlalchemy psycopg2-binary

# Update JobStore to use database
# See: https://docs.masumi.network/documentation/how-to-guides/installing-postgresql-database
```

### Background Processing

Use message queues for job processing:

```python
# Install Celery
pip install celery redis

# Configure Celery worker
# See: https://docs.celeryq.dev/
```

### Monitoring

Add monitoring and logging:

```python
# Install monitoring tools
pip install sentry-sdk prometheus-client

# Configure in main.py
```

### Rate Limiting

Add rate limiting to prevent abuse:

```python
# Install slowapi
pip install slowapi

# Configure rate limits
```

## Troubleshooting

### Payment not confirming

1. Check payment service logs
2. Verify wallet has sufficient ADA
3. Check Cardano network status
4. Verify `identifier_from_purchaser` is valid hex

### Agent not processing jobs

1. Check agent logs: `railway logs`
2. Verify Anthropic API key is valid
3. Check MCP server is accessible
4. Test standalone mode: `python main.py`

### MCP connection errors

1. Verify `CARDANO_MCP_URL` is publicly accessible
2. Check ngrok tunnel is active
3. Test MCP server: `curl https://your-mcp.ngrok-free.app/sse`

## Resources

- [Masumi Documentation](https://docs.masumi.network)
- [CrewAI Documentation](https://docs.crewai.com)
- [Cardano Testnet Faucet](https://docs.cardano.org/cardano-testnets/tools/faucet/)
- [Railway Documentation](https://docs.railway.app)
- [Anthropic API](https://docs.anthropic.com)

## License

MIT License

## Support

For issues and questions:
- Masumi: https://github.com/masumi-network
- CrewAI: https://github.com/joaomdmoura/crewai
