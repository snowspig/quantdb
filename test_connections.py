#!/usr/bin/env python3
# test_connections.py

import os
import sys
import yaml
import pymongo
import tushare as ts
import pandas as pd
from datetime import datetime
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

def load_config():
    """Load configuration from config.yaml file"""
    try:
        config_path = os.path.join(os.path.dirname(__file__), "config", "config.yaml")
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        print(f"{Fore.RED}Error loading configuration: {e}{Style.RESET_ALL}")
        sys.exit(1)

def print_header(text):
    """Print a nicely formatted header"""
    print("\n" + "=" * 60)
    print(f"{Fore.BLUE}{text}{Style.RESET_ALL}")
    print("=" * 60)

def print_success(text):
    """Print a success message"""
    print(f"{Fore.GREEN}✓ {text}{Style.RESET_ALL}")

def print_error(text):
    """Print an error message"""
    print(f"{Fore.RED}✗ {text}{Style.RESET_ALL}")

def print_info(text):
    """Print an info message"""
    print(f"{Fore.CYAN}ℹ {text}{Style.RESET_ALL}")

def test_tushare_connection(config):
    """Test the connection to Tushare API and fetch some data"""
    print_header("Testing Tushare API Connection")
    
    try:
        token = config["tushare"]["token"]
        if token == "your_tushare_token_here":
            print_error("Tushare token not set in config.yaml")
            print_info("Please replace 'your_tushare_token_here' with your actual Tushare token")
            return False
        
        print_info("Initializing Tushare with token...")
        ts.set_token(token)
        pro = ts.pro_api(token)
        
        print_info("Fetching stock basics data...")
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        
        if df is not None and not df.empty:
            print_success("Successfully connected to Tushare API")
            print_success(f"Retrieved {len(df)} stocks from Tushare")
            print_info("Sample data:")
            print(df.head(5).to_string())
            return True
        else:
            print_error("Failed to retrieve data from Tushare")
            return False
            
    except Exception as e:
        print_error(f"Error testing Tushare API: {e}")
        return False

def test_mongodb_connection(config):
    """Test the connection to MongoDB and perform basic operations"""
    print_header("Testing MongoDB Connection")
    
    client = None
    try:
        mongo_config = config["mongodb"]
        host = mongo_config["host"]
        port = mongo_config["port"]
        username = mongo_config["username"]
        password = mongo_config["password"]
        db_name = mongo_config["database"]
        
        uri = f"mongodb://"
        if username and password:
            from urllib.parse import quote_plus
            uri += f"{quote_plus(username)}:{quote_plus(password)}@"
        auth_mechanism = mongo_config.get("auth_mechanism", "SCRAM-SHA-256")
        uri += f"{host}:{port}/{db_name}?authMechanism={auth_mechanism}"
        
        print_info(f"Connecting to MongoDB at {host}:{port}...")
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # Check connection
        client.server_info()
        print_success("Successfully connected to MongoDB server")
        
        # Access database
        db = client[db_name]
        print_success(f"Successfully accessed '{db_name}' database")
        
        # Test collection operations
        test_collection = db["connection_test"]
        
        # Insert a test document
        test_document = {
            "test_id": "conn_test_" + datetime.now().strftime("%Y%m%d%H%M%S"),
            "timestamp": datetime.now(),
            "message": "MongoDB connection test"
        }
        insert_result = test_collection.insert_one(test_document)
        print_success(f"Successfully inserted test document with ID: {insert_result.inserted_id}")
        
        # Read the document back
        found_doc = test_collection.find_one({"_id": insert_result.inserted_id})
        if found_doc:
            print_success("Successfully read test document from MongoDB")
        
        # Update the document
        update_result = test_collection.update_one(
            {"_id": insert_result.inserted_id}, 
            {"$set": {"updated": True}}
        )
        if update_result.modified_count > 0:
            print_success("Successfully updated test document")
        
        # Delete the test document
        delete_result = test_collection.delete_one({"_id": insert_result.inserted_id})
        if delete_result.deleted_count > 0:
            print_success("Successfully deleted test document")
        
        print_info("All MongoDB operations completed successfully")
        return True
    
    except pymongo.errors.ServerSelectionTimeoutError:
        print_error("MongoDB server selection timeout - server may not be running")
        print_info("Check if MongoDB is running on the specified host and port")
        return False
    
    except pymongo.errors.OperationFailure as e:
        print_error(f"MongoDB authentication error: {e}")
        print_info("Check your MongoDB username and password in the config")
        return False
    
    except Exception as e:
        print_error(f"Error testing MongoDB connection: {e}")
        return False
    
    finally:
        if client:
            client.close()
            print_info("MongoDB connection closed")

def main():
    """Main function to run all tests"""
    print_header("QuantDB Connection Tests")
    print_info("Testing connections to Tushare API and MongoDB")
    
    # Load configuration
    config = load_config()
    
    # Track overall success
    success = True
    
    # Test Tushare API connection
    tushare_success = test_tushare_connection(config)
    success = success and tushare_success
    
    print("\n")  # Add spacing between tests
    
    # Test MongoDB connection
    mongodb_success = test_mongodb_connection(config)
    success = success and mongodb_success
    
    # Print summary
    print_header("Test Summary")
    if tushare_success:
        print_success("Tushare API connection: PASSED")
    else:
        print_error("Tushare API connection: FAILED")
    
    if mongodb_success:
        print_success("MongoDB connection: PASSED")
    else:
        print_error("MongoDB connection: FAILED")
    
    if success:
        print_success("All connection tests passed!")
    else:
        print_error("One or more connection tests failed!")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())