# CANDI - Cardano Agent for Network Data Intelligence

A decentralized AI agent service on the Cardano blockchain, implementing the **MIP-003 standard** for secure payments and automated data intelligence.

**Cardano Asia Hackathon - PS-2 (AI Agents on Cardano - Masumi)**

## 🎥 Video Demo

[Watch Demo Video](https://www.youtube.com/watch?v=BETiE8vQ8wo)

---

## 🔗 Quick Links

| Service | URL | Description |
|---------|-----|-------------|
| **CANDI UI** | https://ai.syncgovhub.com/ | Interactive UI for AI agents |
| **MCP Server** | https://cardano-mcp-server-live.up.railway.app/sse | Model Context Protocol server (SSE) |
| **Masumi Admin Dashboard** | https://masumi-payment-service-live.up.railway.app/admin/ | Payment service admin dashboard |
| **Masumi API Docs** | https://masumi-payment-service-live.up.railway.app/docs/ | Payment API documentation |
| **CANDI API Docs** | https://cardano-crewai-agent-live.up.railway.app/docs | CANDI agent API documentation |

---

## 🚀 How It Works - Simple Flow

### Step 1: Register a Job
Create a new job request with your query and a unique identifier.

```bash
POST https://cardano-crewai-agent-live.up.railway.app/start_job
{
  "identifier_from_purchaser": "a1b2c3d4e5f6a7b8c9d0",
  "input_data": {
    "query": "What are the top 5 DReps by voting power on Cardano?"
  }
}
```

**Input Requirements:**
- `identifier_from_purchaser` - Unique hex string (14-26 characters, even length)
- `query` - Natural language question about Cardano blockchain

**What you get back:**
- `job_id` - Track your job status
- `blockchain_identifier` - Used for payment
- Payment details (seller vkey, agent identifier, payment address)

---

### Step 2: Resolve Payment Details
Get payment timing information from Masumi.

```bash
POST https://masumi-payment-service-live.up.railway.app/api/v1/payment/resolve-blockchain-identifier
Headers:
  token: YOUR_MASUMI_API_KEY
  Content-Type: application/json

Body:
{
  "blockchainIdentifier": "from_step_1",
  "network": "Mainnet",
  "includeHistory": "false"
}
```

**Save these values:**
- `payByTime`
- `submitResultTime`
- `unlockTime`
- `externalDisputeUnlockTime`
- `inputHash`

---

### Step 3: Make Purchase
Pay for the service on the Cardano blockchain using Masumi.

**Important:** You need your own **wallet verification key (vkey)** to make payments.

```bash
POST https://masumi-payment-service-live.up.railway.app/api/v1/purchase
Headers:
  token: YOUR_MASUMI_API_KEY
  Content-Type: application/json

Body:
{
  "identifierFromPurchaser": "a1b2c3d4e5f6a7b8c9d0",
  "network": "Mainnet",
  "buyerWalletVkey": "YOUR_WALLET_VKEY_HERE",
  "blockchainIdentifier": "from_step_1",
  "sellerVkey": "from_step_1",
  "agentIdentifier": "from_step_1",
  "payByTime": "from_step_2",
  "submitResultTime": "from_step_2",
  "unlockTime": "from_step_2",
  "externalDisputeUnlockTime": "from_step_2",
  "inputHash": "from_step_2"
}
```

**Cost:** 2 ADA per job (2,000,000 lovelace)

---

### Step 4: Wait for Confirmation
After payment, the blockchain needs ~10 seconds to confirm the transaction.

CANDI automatically:
1. Detects your payment on-chain
2. Verifies the amount and recipient
3. Starts processing your query

---

### Step 5: Poll for Results
Check your job status to get the results.

```bash
GET https://cardano-crewai-agent-live.up.railway.app/status?job_id=YOUR_JOB_ID
```

**Status progression:**
- `pending_payment` → Waiting for payment
- `payment_confirmed` → Payment verified, processing started
- `processing` → CANDI is analyzing Cardano data
- `completed` → Results ready!
- `failed` → Something went wrong (check error field)

**Polling:** Check every 5-10 seconds until status is `completed`

---

### Step 6: Get Your Results
When status is `completed`, the response includes:

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "payment_confirmed": true,
  "result_submitted": true,
  "result": "Based on the latest Cardano governance data, here are the top 5 DReps by voting power: ..."
}
```

**You get:** Detailed blockchain intelligence and data analysis from CANDI.

---

## 📋 Complete Example

### Full Payment Flow with CANDI

```bash
# 1. CREATE JOB
curl -X POST "https://cardano-crewai-agent-live.up.railway.app/start_job" \
  -H "Content-Type: application/json" \
  -d '{
    "identifier_from_purchaser": "a1b2c3d4e5f6a7b8c9d0",
    "input_data": {
      "query": "What are the top 5 DReps by voting power on Cardano?"
    }
  }'

# Save the job_id, blockchain_identifier, seller_vkey, agent_identifier from response

# 2. RESOLVE PAYMENT DETAILS
curl -X POST "https://masumi-payment-service-live.up.railway.app/api/v1/payment/resolve-blockchain-identifier" \
  -H "token: YOUR_MASUMI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "blockchainIdentifier": "FROM_STEP_1",
    "network": "Mainnet",
    "includeHistory": "false"
  }'

# Save: payByTime, submitResultTime, unlockTime, externalDisputeUnlockTime, inputHash

# 3. MAKE PURCHASE
curl -X POST "https://masumi-payment-service-live.up.railway.app/api/v1/purchase" \
  -H "token: YOUR_MASUMI_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "identifierFromPurchaser": "a1b2c3d4e5f6a7b8c9d0",
    "network": "Mainnet",
    "buyerWalletVkey": "YOUR_WALLET_VKEY",
    "blockchainIdentifier": "FROM_STEP_1",
    "sellerVkey": "FROM_STEP_1",
    "agentIdentifier": "FROM_STEP_1",
    "payByTime": "FROM_STEP_2",
    "submitResultTime": "FROM_STEP_2",
    "unlockTime": "FROM_STEP_2",
    "externalDisputeUnlockTime": "FROM_STEP_2",
    "inputHash": "FROM_STEP_2"
  }'

# 4. WAIT 10 SECONDS FOR BLOCKCHAIN CONFIRMATION
sleep 10

# 5. POLL FOR RESULTS (every 5-10 seconds)
curl "https://cardano-crewai-agent-live.up.railway.app/status?job_id=YOUR_JOB_ID"

# 6. GET RESULTS
# When status=completed, the result field contains your blockchain intelligence
```

---

## 🔑 Requirements

### What You Need

1. **Wallet Verification Key (vkey)**
   - Your Cardano wallet's verification key
   - Extract from your wallet using Cardano CLI or wallet software
   - Format: 56-character hexadecimal string
   - **Keep it safe** - needed for all payments

2. **Masumi API Key**
   - Get from Masumi payment service
   - Required for payment API calls
   - Contact Masumi admin for access

3. **Unique Identifier**
   - Generate a random hex string (14-26 characters, even length)
   - Must be unique for each job
   - Example: `openssl rand -hex 10` (generates 20 chars)

4. **2 ADA per Job**
   - Standard price for CANDI queries
   - Paid in Cardano (ADA)
   - Settled on Mainnet or Preprod

---

## 🛠️ Technical Architecture

### Components

1. **CANDI Agent (CrewAI)**
   - Processes Cardano blockchain queries
   - Implements MIP-003 payment protocol
   - 86+ specialized Cardano tools
   - Powered by Claude Sonnet 4

2. **Masumi Payment Service**
   - Handles blockchain payments
   - Validates transactions on-chain
   - Escrow and dispute resolution
   - MIP-003 compliant

3. **MCP Server**
   - Model Context Protocol for Cardano data
   - Server-Sent Events (SSE) for real-time updates
   - Integration with Claude AI

4. **CANDI UI**
   - User-friendly web interface
   - Visual job management
   - Payment tracking and history

---

## 📊 Job Status Flow

```
pending_payment
    ↓
[User makes payment]
    ↓
payment_confirmed
    ↓
processing
    ↓
completed ✅
```

**If something fails:**
```
pending_payment → failed ❌ (payment timeout)
processing → failed ❌ (processing error)
```

**Automatic Refund:**
- If payment is made but job fails, funds are automatically refunded
- MIP-003 escrow protects your payment
- Refund processed on-chain within unlock time
- No manual intervention needed

---

## 🔒 Security & Best Practices

### Do's ✅
- Generate a new unique identifier for each job
- Keep your wallet vkey secure and never share it publicly
- Poll status endpoint every 5 seconds (not faster)
- Wait 10 seconds after payment before polling
- Verify the blockchain_identifier before payment

### Don'ts ❌
- Don't reuse identifiers across multiple jobs
- Don't share your Masumi API key
- Don't poll faster than every 5 seconds (rate limiting)
- Don't skip the payment verification step
- Don't use Preprod vkeys on Mainnet (or vice versa)

---

## 📖 Additional Resources

- **MIP-003 Standard:** https://github.com/masumi-network/mips/blob/main/mip-003.md
- **Cardano Documentation:** https://docs.cardano.org/
- **Masumi Payment Protocol:** https://masumi-payment-service-live.up.railway.app/docs/

---

## 💡 Example Use Cases

### Cardano Governance Analysis
```json
{"query": "What are the top 5 DReps by voting power on Cardano?"}
```
Get insights on governance participation and voting power distribution.

### Token & Market Data
```json
{"query": "What is the current ADA price and market cap?"}
```
Real-time token economics and market intelligence.

### NFT Market Analytics
```json
{"query": "What are the top 3 Cardano NFT collections by volume?"}
```
Track NFT trends, floor prices, and trading volumes.

### DeFi Intelligence
```json
{"query": "What is the current TVL in Cardano DeFi protocols?"}
```
Analyze Total Value Locked across Cardano DeFi ecosystem.

### Network Metrics
```json
{"query": "Show me recent governance proposals on Cardano"}
```
Track on-chain proposals, voting activity, and network health.

---

## 🐛 Troubleshooting

### Common Issues

**"Job not found"**
- Check your job_id is correct
- Job might have expired (24-hour retention)

**"Payment not confirmed"**
- Wait at least 10 seconds after payment
- Check blockchain explorer for transaction status
- Verify you sent to correct seller address

**"Invalid identifier"**
- Must be hexadecimal (0-9, a-f)
- Length must be 14-26 characters (even length)
- Must be unique per job
- Example valid: `a1b2c3d4e5f6a7b8` (16 chars)

**"Timeout waiting for results"**
- Jobs can take 5-15 minutes to complete
- Check status endpoint for error messages
- Contact support if stuck in processing

---

## 📞 Support

For issues or questions:
1. Check the API documentation links above
2. Review the troubleshooting section
3. Contact Masumi support for payment issues
4. Contact agent service support for job issues

---

## 📄 License

This project implements the MIP-003 standard for decentralized AI agent payments on Cardano.

