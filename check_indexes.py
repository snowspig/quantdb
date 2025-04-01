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
    print('\nIndexes for stk_managers collection:')
    collection = db.stk_managers
    for index in collection.list_indexes():
        print(index)
except Exception as e:
    print(f'Error: {str(e)}')
