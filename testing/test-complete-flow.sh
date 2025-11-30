#!/bin/bash

# Cardano CrewAI Agent - Complete Payment Flow Test Script
# This script automates: Job Creation -> Payment Check -> Purchase -> Status Polling

set -e  # Exit on error

# Record start time
SCRIPT_START_TIME=$(date +%s)
SCRIPT_START_TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
AGENT_URL="https://cardano-crewai-agent-live.up.railway.app"
PAYMENT_SERVICE_URL="https://masumi-payment-service-live.up.railway.app/api/v1"
PAYMENT_API_KEY="masumi-payment-admin-smwiu1k72b9avlxr73dwanee"
BUYER_WALLET_VKEY="75026386cfcafce8f6a18c85bce92f0d22081871e8cfcbc08809579e"
NETWORK="Mainnet"

# # Configuration
# AGENT_URL="https://cardano-crewai-agent-preprod.up.railway.app"
# PAYMENT_SERVICE_URL="https://masumi-payment-service-preprod.up.railway.app/api/v1"
# PAYMENT_API_KEY="masumi-payment-admin-f0qu7tme8ql8cax21hwuai7s"
# BUYER_WALLET_VKEY="1dfffeb582b5edff7769bb9d64de025eb7b84135cccd9d143fa712d5"
# NETWORK="Preprod"

# Sample queries
QUERIES=(
    "What are the top 5 DReps by voting power on Cardano?"
    "What are the top 5 DReps by participation rate?"
    "What are the top 3 Cardano NFT collections by volume?"
    "What is the current TVL in Cardano DeFi protocols?"
    "Show me the latest governance proposals on Cardano"
)

# Generate random hex identifier (14-25 chars)
generate_identifier() {
    local length=$((14 + RANDOM % 12))
    openssl rand -hex $(((length + 1) / 2)) | cut -c1-$length
}

# Get random query
get_random_query() {
    local index=$((RANDOM % ${#QUERIES[@]}))
    echo "${QUERIES[$index]}"
}

print_section() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}========================================${NC}"
}

print_elapsed_time() {
    local end_time=$(date +%s)
    local end_timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    local elapsed=$((end_time - SCRIPT_START_TIME))
    local minutes=$((elapsed / 60))
    local seconds=$((elapsed % 60))

    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}  ⏱️  EXECUTION TIME${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo -e "${YELLOW}Started:${NC}  $SCRIPT_START_TIMESTAMP"
    echo -e "${YELLOW}Finished:${NC} $end_timestamp"
    echo -e "${YELLOW}Duration:${NC} ${minutes}m ${seconds}s (${elapsed} seconds)"
    echo -e "${BLUE}========================================${NC}"
}

# Display script start
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  🚀 SCRIPT STARTED${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${YELLOW}Start Time:${NC} $SCRIPT_START_TIMESTAMP"
echo -e "${BLUE}========================================${NC}"

# STEP 1: Create Job
print_section "STEP 1: Creating Job"

IDENTIFIER=$(generate_identifier)
QUERY=$(get_random_query)

echo -e "${YELLOW}📝 Identifier:${NC} $IDENTIFIER"
echo -e "${YELLOW}❓ Query:${NC} $QUERY"

echo -e "\n${BLUE}🚀 Calling POST $AGENT_URL/start_job${NC}"

CREATE_RESPONSE=$(curl -s -X POST "$AGENT_URL/start_job" \
  -H "Content-Type: application/json" \
  -d "{
    \"identifier_from_purchaser\": \"$IDENTIFIER\",
    \"input_data\": {
      \"query\": \"$QUERY\"
    }
  }")

echo "$CREATE_RESPONSE" | python -m json.tool

# Extract values using Python
JOB_ID=$(echo "$CREATE_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('job_id', ''))")
BLOCKCHAIN_ID=$(echo "$CREATE_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('payment_request', {}).get('blockchain_identifier', ''))")
SELLER_VKEY=$(echo "$CREATE_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('payment_request', {}).get('seller_vkey', ''))")
AGENT_ID=$(echo "$CREATE_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('payment_request', {}).get('agent_identifier', ''))")

if [ -z "$JOB_ID" ] || [ -z "$BLOCKCHAIN_ID" ]; then
    echo -e "${RED}❌ Failed to create job${NC}"
    print_elapsed_time
    exit 1
fi

echo -e "\n${GREEN}✅ Job Created Successfully!${NC}"
echo -e "Job ID: ${YELLOW}$JOB_ID${NC}"
echo -e "Blockchain ID: ${YELLOW}${BLOCKCHAIN_ID:0:50}...${NC}"

sleep 2

# STEP 2: Check Payment Status
print_section "STEP 2: Checking Payment Status"

echo -e "${BLUE}🔍 Calling POST $PAYMENT_SERVICE_URL/payment/resolve-blockchain-identifier${NC}"

PAYMENT_STATUS=$(curl -s -X POST "$PAYMENT_SERVICE_URL/payment/resolve-blockchain-identifier" \
  -H "token: $PAYMENT_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"blockchainIdentifier\": \"$BLOCKCHAIN_ID\",
    \"network\": \"$NETWORK\",
    \"includeHistory\": \"false\"
  }")

echo "$PAYMENT_STATUS" | python -m json.tool

# Extract payment timing values
PAY_BY_TIME=$(echo "$PAYMENT_STATUS" | python -c "import sys, json; print(json.load(sys.stdin).get('data', {}).get('payByTime', ''))")
SUBMIT_RESULT_TIME=$(echo "$PAYMENT_STATUS" | python -c "import sys, json; print(json.load(sys.stdin).get('data', {}).get('submitResultTime', ''))")
UNLOCK_TIME=$(echo "$PAYMENT_STATUS" | python -c "import sys, json; print(json.load(sys.stdin).get('data', {}).get('unlockTime', ''))")
EXTERNAL_DISPUTE_TIME=$(echo "$PAYMENT_STATUS" | python -c "import sys, json; print(json.load(sys.stdin).get('data', {}).get('externalDisputeUnlockTime', ''))")
INPUT_HASH=$(echo "$PAYMENT_STATUS" | python -c "import sys, json; print(json.load(sys.stdin).get('data', {}).get('inputHash', ''))")

if [ -z "$INPUT_HASH" ]; then
    echo -e "${RED}❌ Failed to get payment status${NC}"
    print_elapsed_time
    exit 1
fi

echo -e "\n${GREEN}✅ Payment Status Retrieved${NC}"

sleep 2

# STEP 3: Make Purchase
print_section "STEP 3: Making Purchase"

echo -e "${BLUE}💳 Calling POST $PAYMENT_SERVICE_URL/purchase${NC}"
echo -e "${YELLOW}Amount: 2 ADA (2,000,000 lovelace)${NC}"

PURCHASE_RESPONSE=$(curl -s -X POST "$PAYMENT_SERVICE_URL/purchase" \
  -H "token: $PAYMENT_API_KEY" \
  -H "Content-Type: application/json" \
  -d "{
    \"identifierFromPurchaser\": \"$IDENTIFIER\",
    \"network\": \"$NETWORK\",
    \"sellerVkey\": \"$SELLER_VKEY\",
    \"agentIdentifier\": \"$AGENT_ID\",
    \"buyerWalletVkey\": \"$BUYER_WALLET_VKEY\",
    \"blockchainIdentifier\": \"$BLOCKCHAIN_ID\",
    \"payByTime\": \"$PAY_BY_TIME\",
    \"submitResultTime\": \"$SUBMIT_RESULT_TIME\",
    \"unlockTime\": \"$UNLOCK_TIME\",
    \"externalDisputeUnlockTime\": \"$EXTERNAL_DISPUTE_TIME\",
    \"inputHash\": \"$INPUT_HASH\"
  }")

echo "$PURCHASE_RESPONSE" | python -m json.tool

PURCHASE_STATUS=$(echo "$PURCHASE_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('status', ''))")

if [ "$PURCHASE_STATUS" != "success" ]; then
    echo -e "${RED}❌ Purchase failed${NC}"
    print_elapsed_time
    exit 1
fi

echo -e "\n${GREEN}✅ Purchase Successful!${NC}"
echo -e "${YELLOW}⏳ Waiting 10 seconds for blockchain confirmation...${NC}"
sleep 10

# STEP 4: Poll Job Status
print_section "STEP 4: Monitoring Job Status"

echo -e "${BLUE}🔄 Polling status for job: $JOB_ID${NC}"
echo -e "${YELLOW}Checking every 5 seconds...${NC}\n"

POLL_COUNT=0
MAX_POLLS=120  # 10 minutes max

while [ $POLL_COUNT -lt $MAX_POLLS ]; do
    STATUS_RESPONSE=$(curl -s "$AGENT_URL/status?job_id=$JOB_ID")

    STATUS=$(echo "$STATUS_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('status', 'unknown'))" 2>/dev/null || echo "error")
    PAYMENT_CONFIRMED=$(echo "$STATUS_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('payment_confirmed', False))" 2>/dev/null || echo "false")
    RESULT_SUBMITTED=$(echo "$STATUS_RESPONSE" | python -c "import sys, json; rs = json.load(sys.stdin).get('result_submitted'); print('N/A' if rs is None else rs)" 2>/dev/null || echo "N/A")

    # Print status with emojis
    TIMESTAMP=$(date '+%H:%M:%S')

    case $STATUS in
        "pending_payment")
            STATUS_EMOJI="⏳"
            ;;
        "payment_confirmed")
            STATUS_EMOJI="✅"
            ;;
        "processing")
            STATUS_EMOJI="🔄"
            ;;
        "completed")
            STATUS_EMOJI="✅"
            ;;
        "failed")
            STATUS_EMOJI="❌"
            ;;
        *)
            STATUS_EMOJI="❓"
            ;;
    esac

    PAYMENT_EMOJI="⏳"
    if [ "$PAYMENT_CONFIRMED" = "True" ]; then
        PAYMENT_EMOJI="✅"
    fi

    echo -e "[$TIMESTAMP] $STATUS_EMOJI Status: ${YELLOW}$STATUS${NC} | Payment: $PAYMENT_EMOJI | Result Submitted: $RESULT_SUBMITTED"

    # Check if completed
    if [ "$STATUS" = "completed" ]; then
        echo -e "\n${GREEN}========================================${NC}"
        echo -e "${GREEN}🎉 JOB COMPLETED SUCCESSFULLY!${NC}"
        echo -e "${GREEN}========================================${NC}"

        RESULT=$(echo "$STATUS_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('result', 'No result'))")

        echo -e "\n${BLUE}📊 Result:${NC}\n"
        echo "$RESULT"
        echo -e "\n${GREEN}========================================${NC}"

        if [ "$RESULT_SUBMITTED" = "True" ]; then
            echo -e "${GREEN}✅ Result submitted to Masumi - Funds released to seller!${NC}"
        else
            echo -e "${YELLOW}⚠️ Result not submitted - Check logs${NC}"
        fi

        print_elapsed_time
        exit 0
    fi

    # Check if failed
    if [ "$STATUS" = "failed" ]; then
        echo -e "\n${RED}❌ Job Failed!${NC}"
        ERROR=$(echo "$STATUS_RESPONSE" | python -c "import sys, json; print(json.load(sys.stdin).get('error', 'Unknown'))")
        echo -e "Error: $ERROR"
        print_elapsed_time
        exit 1
    fi

    sleep 5
    POLL_COUNT=$((POLL_COUNT + 1))
done

echo -e "\n${YELLOW}⏱️ Timeout: Job did not complete within 10 minutes${NC}"
print_elapsed_time
exit 1
