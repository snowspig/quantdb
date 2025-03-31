#!/usr/bin/env python
import sys
import urllib.parse
from pymongo import MongoClient

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
        
        # Count documents
        doc_count = collection.count_documents({})
        print(f"Total documents in previous_name collection: {doc_count}")
        
        # Get first 5 documents
        print("\nFirst 5 documents:")
        for i, doc in enumerate(collection.find().limit(5)):
            # Remove _id for cleaner output
            if '_id' in doc:
                del doc['_id']
            print(f"\nDocument {i+1}:")
            for key, value in doc.items():
                print(f"  {key}: {value}")
                
        print("\nDatabase query completed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
