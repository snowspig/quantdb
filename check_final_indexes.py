from storage.mongodb_client import MongoDBClient

def check_div_indexes():
    client = MongoDBClient(db_name='tushare_data')
    if not client.connect():
        print("Failed to connect to MongoDB")
        return
    
    db = client.get_db()
    div_collection = db['div']
    
    # Check total count
    total_count = div_collection.count_documents({})
    print(f"Total records in div collection: {total_count}")
    
    # Check all indexes now
    print("\nIndexes on div collection:")
    for idx_name, idx_info in div_collection.index_information().items():
        print(f"{idx_name}: {idx_info}")
    
    # Check for new records
    print("\nRecords for 600002.SH:")
    for doc in div_collection.find({'ts_code': '600002.SH'}).sort('ex_date', -1).limit(3):
        print(doc)

if __name__ == "__main__":
    check_div_indexes()
