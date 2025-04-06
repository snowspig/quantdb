from storage.mongodb_client import MongoDBClient
from pymongo import MongoClient

def check_weekly_collection():
    client = MongoDBClient()
    # Connect method is synchronous
    connected = client.connect()
    
    if connected:
        db = client.client[client.db_name]
        collection = db['weekly']
        
        # Count documents
        count = collection.count_documents({})
        print(f'Number of documents in the weekly collection: {count}')
        
        if count > 0:
            sample = collection.find_one()
            print('Sample document from weekly collection:')
            for key, value in sample.items():
                print(f'  {key}: {value}')
        
        client.close()
    else:
        print('Failed to connect to MongoDB')

if __name__ == '__main__':
    check_weekly_collection()