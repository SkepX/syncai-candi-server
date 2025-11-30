#!/usr/bin/env python3
"""
Complete Payment Flow Test Script
Automates job creation, payment, and result retrieval
"""

import requests
import json
import time
import secrets
from datetime import datetime

# Configuration
AGENT_URL = "https://cardano-crewai-agent.up.railway.app"
PAYMENT_SERVICE_URL = "https://masumi-payment-service-production-4134.up.railway.app/api/v1"
PAYMENT_API_KEY = "masumi-payment-ubthxo2rl1339psg23wkk7uk"
BUYER_WALLET_VKEY = "1dfffeb582b5edff7769bb9d64de025eb7b84135cccd9d143fa712d5"
NETWORK = "Preprod"

# Sample queries
SAMPLE_QUERIES = [
    "What are the top 5 DReps by voting power on Cardano?",
    "What are the top 5 DReps by participation rate?",
    "What are the top 3 Cardano NFT collections by volume?",
    "What is the current TVL in Cardano DeFi protocols?",
    "Show me the latest governance proposals on Cardano"
]

def generate_hex_identifier(min_length=14, max_length=25):
    """Generate random hex identifier between 14-25 characters"""
    length = secrets.randbelow(max_length - min_length + 1) + min_length
    # Generate even number of hex digits
    if length % 2 == 1:
        length += 1
    return secrets.token_hex(length // 2)[:length]

def print_section(title):
    """Print formatted section header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def print_json(data):
    """Pretty print JSON data"""
    print(json.dumps(data, indent=2))

def step1_create_job():
    """Step 1: Create a job and get payment details"""
    print_section("STEP 1: Creating Job")

    # Generate random identifier and query
    identifier = generate_hex_identifier()
    query = secrets.choice(SAMPLE_QUERIES)

    print(f"📝 Identifier: {identifier}")
    print(f"❓ Query: {query}")

    payload = {
        "identifier_from_purchaser": identifier,
        "input_data": {
            "query": query
        }
    }

    print(f"\n🚀 Calling POST {AGENT_URL}/start_job")
    response = requests.post(
        f"{AGENT_URL}/start_job",
        headers={"Content-Type": "application/json"},
        json=payload
    )

    if response.status_code not in [200, 201]:
        print(f"❌ Failed to create job: {response.status_code}")
        print(response.text)
        return None

    data = response.json()
    print("\n✅ Job Created Successfully!")
    print(f"Job ID: {data['job_id']}")
    print(f"Blockchain ID: {data['payment_request']['blockchain_identifier'][:50]}...")

    return {
        "job_id": data["job_id"],
        "blockchain_identifier": data["payment_request"]["blockchain_identifier"],
        "identifier_from_purchaser": identifier,
        "payment_address": data["payment_request"]["payment_address"],
        "seller_vkey": data["payment_request"]["seller_vkey"],
        "agent_identifier": data["payment_request"]["agent_identifier"]
    }

def step2_check_payment_status(blockchain_identifier):
    """Step 2: Check payment status before purchase"""
    print_section("STEP 2: Checking Payment Status")

    payload = {
        "blockchainIdentifier": blockchain_identifier,
        "network": NETWORK,
        "includeHistory": "false"
    }

    print(f"🔍 Calling POST {PAYMENT_SERVICE_URL}/payment/resolve-blockchain-identifier")
    response = requests.post(
        f"{PAYMENT_SERVICE_URL}/payment/resolve-blockchain-identifier",
        headers={
            "token": PAYMENT_API_KEY,
            "Content-Type": "application/json"
        },
        json=payload
    )

    if response.status_code != 200:
        print(f"❌ Failed to check payment: {response.status_code}")
        print(response.text)
        return None

    data = response.json()

    if data.get("status") == "success":
        payment_data = data["data"]
        print("\n✅ Payment Status Retrieved")
        print(f"Status: {payment_data.get('NextAction', {}).get('requestedAction')}")
        print(f"Pay By Time: {payment_data.get('payByTime')}")
        print(f"Submit Result Time: {payment_data.get('submitResultTime')}")

        return {
            "payByTime": payment_data.get("payByTime"),
            "submitResultTime": payment_data.get("submitResultTime"),
            "unlockTime": payment_data.get("unlockTime"),
            "externalDisputeUnlockTime": payment_data.get("externalDisputeUnlockTime"),
            "inputHash": payment_data.get("inputHash")
        }

    return None

def step3_make_purchase(job_data, payment_data):
    """Step 3: Make purchase using Masumi API"""
    print_section("STEP 3: Making Purchase")

    payload = {
        "identifierFromPurchaser": job_data["identifier_from_purchaser"],
        "network": NETWORK,
        "sellerVkey": job_data["seller_vkey"],
        "agentIdentifier": job_data["agent_identifier"],
        "buyerWalletVkey": BUYER_WALLET_VKEY,
        "blockchainIdentifier": job_data["blockchain_identifier"],
        "payByTime": payment_data["payByTime"],
        "submitResultTime": payment_data["submitResultTime"],
        "unlockTime": payment_data["unlockTime"],
        "externalDisputeUnlockTime": payment_data["externalDisputeUnlockTime"],
        "inputHash": payment_data["inputHash"]
    }

    print(f"💳 Calling POST {PAYMENT_SERVICE_URL}/purchase")
    print(f"Amount: 2 ADA (2,000,000 lovelace)")

    response = requests.post(
        f"{PAYMENT_SERVICE_URL}/purchase",
        headers={
            "token": PAYMENT_API_KEY,
            "Content-Type": "application/json"
        },
        json=payload
    )

    if response.status_code not in [200, 201]:
        print(f"❌ Purchase failed: {response.status_code}")
        print(response.text)
        return False

    data = response.json()

    if data.get("status") == "success":
        purchase_data = data["data"]
        print("\n✅ Purchase Successful!")
        print(f"Next Action: {purchase_data.get('NextAction', {}).get('requestedAction')}")
        print(f"Transaction will be submitted to blockchain...")
        return True

    return False

def step4_poll_job_status(job_id):
    """Step 4: Poll job status until completed"""
    print_section("STEP 4: Monitoring Job Status")

    print(f"🔄 Polling status for job: {job_id}")
    print("Checking every 5 seconds...\n")

    poll_count = 0
    max_polls = 120  # 10 minutes max (120 * 5 seconds)

    while poll_count < max_polls:
        response = requests.get(f"{AGENT_URL}/status?job_id={job_id}")

        if response.status_code != 200:
            print(f"❌ Status check failed: {response.status_code}")
            time.sleep(5)
            poll_count += 1
            continue

        data = response.json()
        status = data.get("status")
        payment_confirmed = data.get("payment_confirmed", False)
        result_submitted = data.get("result_submitted")

        # Print status update
        timestamp = datetime.now().strftime("%H:%M:%S")
        status_emoji = {
            "pending_payment": "⏳",
            "payment_confirmed": "✅",
            "processing": "🔄",
            "completed": "✅",
            "failed": "❌"
        }.get(status, "❓")

        print(f"[{timestamp}] {status_emoji} Status: {status} | Payment: {'✅' if payment_confirmed else '⏳'} | Result Submitted: {result_submitted if result_submitted is not None else 'N/A'}")

        # Check if completed
        if status == "completed":
            print("\n" + "="*80)
            print("🎉 JOB COMPLETED SUCCESSFULLY!")
            print("="*80)
            print(f"\n📊 Result:\n")
            print(data.get("result", "No result available"))
            print("\n" + "="*80)

            if result_submitted:
                print("✅ Result submitted to Masumi - Funds released to seller!")
            else:
                print("⚠️ Result not submitted - Check logs")

            return data

        # Check if failed
        if status == "failed":
            print(f"\n❌ Job Failed!")
            print(f"Error: {data.get('error', 'Unknown error')}")
            return data

        # Wait before next poll
        time.sleep(5)
        poll_count += 1

    print("\n⏱️ Timeout: Job did not complete within 10 minutes")
    return None

def print_elapsed_time(start_time):
    """Print execution time statistics"""
    end_time = time.time()
    elapsed = end_time - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print("\n" + "="*80)
    print("  ⏱️  EXECUTION TIME")
    print("="*80)
    print(f"Started:  {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Finished: {datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {minutes}m {seconds}s ({int(elapsed)} seconds)")
    print("="*80)

def main():
    """Main execution flow"""
    # Record start time
    script_start_time = time.time()
    start_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    print("\n" + "="*80)
    print("  🚀 SCRIPT STARTED")
    print("="*80)
    print(f"Start Time: {start_timestamp}")
    print("="*80)

    print("\n" + "="*80)
    print("  🚀 CARDANO CREWAI AGENT - COMPLETE PAYMENT FLOW TEST")
    print("="*80)
    print(f"Agent URL: {AGENT_URL}")
    print(f"Payment Service: {PAYMENT_SERVICE_URL}")
    print(f"Network: {NETWORK}")
    print("="*80)

    try:
        # Step 1: Create Job
        job_data = step1_create_job()
        if not job_data:
            print("❌ Failed at Step 1: Job Creation")
            print_elapsed_time(script_start_time)
            return

        # Small delay
        time.sleep(2)

        # Step 2: Check Payment Status
        payment_data = step2_check_payment_status(job_data["blockchain_identifier"])
        if not payment_data:
            print("❌ Failed at Step 2: Payment Status Check")
            print_elapsed_time(script_start_time)
            return

        # Small delay
        time.sleep(2)

        # Step 3: Make Purchase
        purchase_success = step3_make_purchase(job_data, payment_data)
        if not purchase_success:
            print("❌ Failed at Step 3: Purchase")
            print_elapsed_time(script_start_time)
            return

        # Wait for blockchain confirmation
        print("\n⏳ Waiting 10 seconds for blockchain confirmation...")
        time.sleep(10)

        # Step 4: Poll Job Status
        result = step4_poll_job_status(job_data["job_id"])

        if result and result.get("status") == "completed":
            print("\n✅ COMPLETE FLOW SUCCESSFUL! 🎉")
            print(f"Job ID: {job_data['job_id']}")
            print(f"Payment Confirmed: {result.get('payment_confirmed')}")
            print(f"Result Submitted: {result.get('result_submitted')}")
            print_elapsed_time(script_start_time)
        else:
            print("\n❌ Flow did not complete successfully")
            print_elapsed_time(script_start_time)

    except KeyboardInterrupt:
        print("\n\n⚠️ Script interrupted by user")
        print_elapsed_time(script_start_time)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print_elapsed_time(script_start_time)

if __name__ == "__main__":
    main()
