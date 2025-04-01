from storage.mongodb_client import MongoDBClient

def check_div_collection():
    client = MongoDBClient(db_name='tushare_data')
    if not client.connect():
        print("Failed to connect to MongoDB")
        return
    
    db = client.get_db()
    div_collection = db['div']
    
    # Check total count
    total_count = div_collection.count_documents({})
    print(f"Total records in div collection: {total_count}")
    
    # Check records for 600001.SH
    print("\nRecords for 600001.SH:")
    counter = 0
    for doc in div_collection.find({'ts_code': '600001.SH'}).sort('ex_date', -1).limit(5):
        print(doc)
        counter += 1
    print(f"Found {counter} records for 600001.SH")
    
    # Check indexes
    print("\nIndexes:")
    for idx_name, idx_info in div_collection.index_information().items():
        print(f"{idx_name}: {idx_info}")
        
    # Check if compound index on ts_code and ann_date exists
    compound_index_exists = False
    for idx_name, idx_info in div_collection.index_information().items():
        if 'key' in idx_info and len(idx_info['key']) >= 2:
            # Check if index contains both ts_code and ann_date
            keys = [k[0] for k in idx_info['key']]
            if 'ts_code' in keys and 'ann_date' in keys:
                compound_index_exists = True
                print(f"\nFound compound index with ts_code and ann_date: {idx_name}")
                break
    
    if not compound_index_exists:
        print("\nNo compound index found with both ts_code and ann_date")
        print("Creating index now...")
        try:
            div_collection.create_index(
                [("ts_code", 1), ("ann_date", 1)],
                unique=True,
                background=True,
                name="ts_code_ann_date_unique"
            )
            print("Successfully created compound unique index on (ts_code, ann_date)")
        except Exception as e:
            print(f"Error creating index: {e}")

if __name__ == "__main__":
    check_div_collection()
