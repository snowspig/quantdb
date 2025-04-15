#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Example script showing how to use the stock_basic_fetcher.py module
to fetch stock basic information and perform simple analysis.

This script demonstrates:
1. Fetching stock data from Tushare
2. Filtering by market code
3. Basic data analysis
"""

import subprocess
import sys
import pandas as pd
from pymongo import MongoClient

# MongoDB connection settings
MONGO_URI = "mongodb://adminUser:MongoAdmin%402025%21@106.14.185.239:27017/?authSource=admin&authMechanism=SCRAM-SHA-1"
DB_NAME = "tushare_data"
COLLECTION = "stock_basic"


def fetch_stock_data(market_codes="SS,SZ"):
    """
    Fetch stock data using stock_basic_fetcher.py
    
    Args:
        market_codes (str): Comma-separated market codes (SS for Shanghai, SZ for Shenzhen)
    
    Returns:
        int: Return code from the subprocess
    """
    print(f"Fetching stock data for markets: {market_codes}")
    cmd = [
        "python", 
        "stock_basic_fetcher.py", 
        "--verbose", 
        "--db-name", DB_NAME, 
        "--collection-name", COLLECTION,
        "--market-codes", market_codes
    ]
    
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    print("Command output:")
    print(result.stdout)
    
    if result.returncode != 0:
        print("Error output:")
        print(result.stderr)
    
    return result.returncode


def analyze_stock_data():
    """
    Analyze stock data from MongoDB
    """
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION]
    
    # Get counts
    total_count = collection.count_documents({})
    sse_count = collection.count_documents({"exchange": "SSE"})
    szse_count = collection.count_documents({"exchange": "SZSE"})
    
    print(f"\nStock Data Analysis:")
    print(f"Total stocks: {total_count}")
    print(f"Shanghai (SSE) stocks: {sse_count}")
    print(f"Shenzhen (SZSE) stocks: {szse_count}")
    
    # Convert to DataFrame for more advanced analysis
    cursor = collection.find(
        {}, 
        {"_id": 0, "ts_code": 1, "name": 1, "exchange": 1, "list_date": 1, "list_board_name": 1}
    )
    df = pd.DataFrame(list(cursor))
    
    if not df.empty:
        # Board distribution
        board_counts = df["list_board_name"].value_counts()
        print("\nStock distribution by board:")
        for board, count in board_counts.items():
            print(f"{board}: {count}")
        
        # Listing year distribution
        if "list_date" in df.columns:
            df["list_year"] = df["list_date"].str[:4]
            year_counts = df["list_year"].value_counts().sort_index()
            print("\nStocks by listing year (top 10):")
            for year, count in year_counts.head(10).items():
                print(f"{year}: {count}")
    else:
        print("No data found in the collection.")


def main():
    """
    Main function to demonstrate stock_basic_fetcher usage
    """
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Example script for stock_basic_fetcher")
    parser.add_argument("--fetch", action="store_true", help="Fetch stock data")
    parser.add_argument("--market", default="SS,SZ", help="Market codes (default: SS,SZ)")
    args = parser.parse_args()
    
    if args.fetch:
        # Fetch stock data
        result = fetch_stock_data(args.market)
        if result != 0:
            print("Failed to fetch stock data")
            return result
    
    # Analyze the data
    analyze_stock_data()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
