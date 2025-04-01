from pymongo import MongoClient
import yaml
import urllib.parse

with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

username = urllib.parse.quote_plus(config['mongodb']['username'])
password = urllib.parse.quote_plus(config['mongodb']['password']) 
host = config['mongodb']['host']
port = config['mongodb']['port']
auth_source = config.get('mongodb', {}).get('auth_source', 'admin')

uri = f'mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}&authMechanism=SCRAM-SHA-1'

try:
    client = MongoClient(uri)
    print('Connected to MongoDB successfully')
    db = client['tushare_data']
    
    # List all available collections
    print('\n=== All available collections in database: ===')
    all_collections = db.list_collection_names()
    for coll in sorted(all_collections):
        print(f'  - {coll}')
    
    # Check indexes for all relevant collections
    collections = ['stk_managers', 'previous_name', 'trade_cal', 'stock_basic', 'stock_company']
    
    for collection_name in collections:
        print(f'\n=== Indexes for {collection_name} collection: ===')
        try:
            if collection_name in all_collections:
                collection = db[collection_name]
                index_list = list(collection.list_indexes())
                if index_list:
                    for index in index_list:
                        print(index)
                else:
                    print(f'  No indexes found in {collection_name}')
            else:
                print(f'  Collection {collection_name} does not exist in the database')
        except Exception as e:
            print(f'Error listing indexes for {collection_name}: {str(e)}')
    
except Exception as e:
    print(f'Error: {str(e)}')
