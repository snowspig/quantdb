#!/usr/bin/env python
import os
import sys
import json
import requests
import asyncio
from pathlib import Path

# Add project root directory to Python path
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# Import project modules
from config import config_manager
from data_fetcher.tushare_client import TushareClient

async def test_api():
    # Get configuration
    token = config_manager.get_tushare_token()
    api_url = config_manager.get_tushare_api_url()
    
    print(f"Using API URL: {api_url}")
    print(f"Using token: {token}")
    
    # Test direct API request
    data = {
        "api_name": "weekly",
        "token": token,
        "params": {
            "trade_date": "20250314"
        },
        "fields": ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
    }
    
    print("\nSending direct API request...")
    response = requests.post(api_url, json=data)
    print(f"Response status: {response.status_code}")
    if response.status_code == 200:
        result = response.json()
        print(f"API Response: {json.dumps(result, indent=2)}")
    else:
        print(f"Request failed: {response.text}")
    
    # Test using TushareClient
    print("\nTesting with TushareClient...")
    client = TushareClient(token=token, api_url=api_url)
    result = client.request("weekly", {"trade_date": "20250314"}, 
                          ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"])
    print(f"Client response: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(test_api())