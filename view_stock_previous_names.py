#!/usr/bin/env python
import sys
import urllib.parse
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def main():
    try:
        # MongoDB connection info
        username = "adminUser"
        password = urllib.parse.quote_plus("MongoAdmin@2025!")
        host = "106.14.185.239"
        port = 27017
        auth_source = "admin"
        
        # Create connection string
        conn_str = f"mongodb://{username}:{password}@{host}:{port}/{auth_source}?authSource={auth_source}"
        print(f"Connecting to MongoDB: {host}:{port}")
        
        # Connect to MongoDB
        client = MongoClient(conn_str)
        db = client["tushare_data"]
        collection = db["previous_name"]
        
        # Get data from MongoDB
        cursor = collection.find({}, {'_id': 0})
        df = pd.DataFrame(list(cursor))
        
        # Basic information about the dataset
        print(f"Dataset shape: {df.shape}")
        print(f"\nColumns: {df.columns.tolist()}")
        
        # Count entries by market code
        market_counts = df['market_code'].value_counts()
        print("\nEntries by market code:")
        for market, count in market_counts.items():
            print(f"  {market}: {count}")
        
        # Count known change reasons
        if 'changereason' in df.columns:
            # Exclude NaN values
            valid_reasons = df.dropna(subset=['changereason'])
            reason_counts = valid_reasons['changereason'].value_counts()
            print("\nEntries by change reason code:")
            for reason, count in reason_counts.items():
                print(f"  {reason}: {count}")
        
        # Timeline of name changes (by year)
        if 'begindate' in df.columns:
            # Convert begindate to datetime and extract year
            df['year'] = pd.to_datetime(df['begindate'], format='%Y%m%d', errors='coerce').dt.year
            year_counts = df['year'].value_counts().sort_index()
            print("\nName changes by year:")
            for year, count in year_counts.items():
                print(f"  {year}: {count}")
            
            # Create visualization
            plt.figure(figsize=(12, 6))
            plt.bar(year_counts.index.astype(str), year_counts.values)
            plt.title('Stock Name Changes by Year')
            plt.xlabel('Year')
            plt.ylabel('Number of Name Changes')
            plt.xticks(rotation=90)
            plt.tight_layout()
            plt.savefig('stock_name_changes_by_year.png')
            print("\nGenerated chart: stock_name_changes_by_year.png")
            
        print("\nAnalysis completed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
