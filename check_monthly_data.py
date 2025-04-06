from storage.mongodb_client import MongoDBClient
import asyncio
import pymongo

def check_monthly_data():
    client = MongoDBClient()
    # The connect method returns a boolean, not a coroutine
    client.connect()
    collection = client.get_collection('monthly')
    # Find without await as it's not async
    sample_doc = collection.find_one()
    print('Sample document from monthly collection:')
    print(sample_doc)
    if sample_doc:
        # Check if market_code is in the document
        if 'market_code' in sample_doc:
            print("WARNING: market_code field found in document")
        else:
            print("SUCCESS: No market_code field found in the document")
        # Print all fields for verification
        print("\nAll fields in document:")
        for field in sample_doc.keys():
            print(f"- {field}")
    else:
        print("No documents found in the monthly collection")

if __name__ == '__main__':
    check_monthly_data()
