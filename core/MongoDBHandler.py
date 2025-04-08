import pymongo
from urllib.parse import quote_plus

class MongoDBHandler:
    def __init__(self, config):
        self.config = config
        self.client = None

    def connect(self):
        try:
            mongo_config = self.config['mongodb']
            host = mongo_config['host']
            port = mongo_config['port']
            username = mongo_config['username']
            password = mongo_config['password']
            db_name = mongo_config['database']

            uri = f"mongodb://"
            if username and password:
                uri += f"{quote_plus(username)}:{quote_plus(password)}@"
            uri += f"{host}:{port}/?authSource={mongo_config['auth_source']}&authMechanism={mongo_config['auth_mechanism']}"

            self.client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=mongo_config['options']['server_selection_timeout_ms'])
            self.client.server_info()  # Force connection on a request as the
            print("Successfully connected to MongoDB server")

            return self.client[db_name]
        except pymongo.errors.ServerSelectionTimeoutError:
            print("MongoDB server selection timeout - server may not be running")
            return None
        except pymongo.errors.OperationFailure as e:
            print(f"MongoDB authentication error: {e}")
            return None
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            return None

    def close(self):
        if self.client:
            self.client.close()
            print("MongoDB connection closed")
