#!/usr/bin/env python
import yaml
import urllib.parse
from pymongo import MongoClient
from pprint import pprint

# Load configuration
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Get MongoDB configuration
mongodb = config['mongodb']
username = urllib.parse.quote_plus(mongodb['username'])
password = urllib.parse.quote_plus(mongodb['password'])
host = mongodb['host']
port = mongodb['port']

# Connect to MongoDB
uri = f"mongodb://{username}:{password}@{host}:{port}/admin?authSource=admin"
print(f"Connecting to: {uri.replace(password, '******')}")

client = MongoClient(uri)

# Check connection
try:
    client.admin.command('ping')
    print("MongoDB connection successful!")
except Exception as e:
    print(f"MongoDB connection failed: {str(e)}")
    exit(1)

# List all databases
print("\nAvailable databases:")
for db_name in client.list_database_names():
    print(f"- {db_name}")

# Check if tushare_data database exists
if 'tushare_data' in client.list_database_names():
    db = client['tushare_data']
    print("\nCollections in tushare_data database:")
    for collection_name in db.list_collection_names():
        count = db[collection_name].count_documents({})
        print(f"- {collection_name} ({count} documents)")
        
    # Show a sample document from stock_basic collection
    if 'stock_basic' in db.list_collection_names():
        print("\nSample document from stock_basic collection:")
        sample = db['stock_basic'].find_one()
        pprint(sample)

client.close()