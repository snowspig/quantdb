from storage.mongodb_client import MongoDBClient

def check_div_collection():
    client = MongoDBClient(db_name='tushare_data')
    if not client.connect():
        print("Failed to connect to MongoDB")
        return
    
    db = client.get_db()
    collection_names = db.list_collection_names()
    
    print(f"All collections: {collection_names}")
    print(f"Div collection exists: {'div' in collection_names}")
    
    if 'div' in collection_names:
        div_collection = db['div']
        count = div_collection.count_documents({})
        print(f"Number of records in div collection: {count}")
        
        # Check indexes
        indexes = div_collection.index_information()
        print(f"Indexes in div collection: {indexes}")
        
        # Show sample documents
        if count > 0:
            print("\nSample documents:")
            for doc in div_collection.find().limit(3):
                print(doc)

if __name__ == "__main__":
    check_div_collection()
