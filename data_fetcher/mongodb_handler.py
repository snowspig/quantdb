import pymongo
from datetime import datetime
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure
from urllib.parse import quote_plus
import yaml
import os

class MongoDBHandler:
    def __init__(self, config_path="config/config.yaml"):
        self.config = self.load_config(config_path)
        self.client = None
        self.db = None

    def load_config(self, config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            return config
        except Exception as e:
            raise Exception(f"Error loading configuration: {e}")

    def connect(self):
        try:
            mongo_config = self.config["mongodb"]
            host = mongo_config["host"]
            port = mongo_config["port"]
            username = mongo_config["username"]
            password = mongo_config["password"]
            db_name = mongo_config["database"]
            auth_mechanism = mongo_config.get("auth_mechanism", "SCRAM-SHA-256")

            uri = f"mongodb://"
            if username and password:
                uri += f"{quote_plus(username)}:{quote_plus(password)}@"
            uri += f"{host}:{port}/?authMechanism={auth_mechanism}"

            self.client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
            self.client.server_info()  # Check connection
            self.db = self.client[db_name]
            print("Successfully connected to MongoDB server and accessed database")
        except ServerSelectionTimeoutError:
            raise Exception("MongoDB server selection timeout - server may not be running")
        except OperationFailure as e:
            raise Exception(f"MongoDB authentication error: {e}")
        except Exception as e:
            raise Exception(f"Error connecting to MongoDB: {e}")

    def insert_document(self, collection_name, document):
        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            return result.inserted_id
        except Exception as e:
            raise Exception(f"Error inserting document: {e}")

    def update_document(self, collection_name, query, update):
        try:
            collection = self.db[collection_name]
            result = collection.update_one(query, {"$set": update})
            return result.modified_count
        except Exception as e:
            raise Exception(f"Error updating document: {e}")

    def delete_document(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            result = collection.delete_one(query)
            return result.deleted_count
        except Exception as e:
            raise Exception(f"Error deleting document: {e}")

    def close_connection(self):
        if self.client:
            self.client.close()
            print("MongoDB connection closed")