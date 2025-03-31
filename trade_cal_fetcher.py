#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ultimate Trade Calendar Fetcher with Intelligent Batch Processing

This script fetches trading calendar data from TuShare API and stores it in MongoDB.
It implements advanced intelligent batch processing with real-time performance metrics,
adaptive batch sizing, and comprehensive error handling.

Features:
- Multiple batch strategies (year, exchange, exchange_year)
- Intelligent and adaptive batch sizing
- Real-time performance metrics and reporting
- Automatic retries with exponential backoff
- Verification and data integrity checks
- Memory-efficient bulk operations
"""

import argparse
import logging
import time
import yaml
import json
import sys
import os
import re
import traceback
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Any, Union, Tuple, Optional, Callable
from functools import wraps
import concurrent.futures

# Third-party imports
import requests
import pymongo
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError, ConnectionFailure

# Import intelligent batch processing modules
from intelligent_batch_processing import calculate_intelligent_batches, PerformanceTracker, generate_performance_report
from adaptive_batch import PerformanceTracker as AdaptiveTracker
from adaptive_batch import generate_performance_report as adaptive_report
from adaptive_batch import calculate_adaptive_batch_size, determine_optimal_strategy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default paths
CONFIG_PATH = "config/config.yaml"
INTERFACE_CONFIG_PATH = "config/interfaces/trade_cal.json"


# Retry decorator for API calls
def retry_with_backoff(max_retries=3, base_delay=1):
    """
    Decorator for retrying functions with exponential backoff
    
    Args:
        max_retries (int): Maximum number of retries
        base_delay (float): Initial delay in seconds
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = base_delay
            
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Max retries ({max_retries}) exceeded")
                        raise
                    
                    wait = delay * (2 ** (retries - 1))  # Exponential backoff
                    logger.warning(f"Retry {retries}/{max_retries} after {wait:.2f}s due to: {str(e)}")
                    time.sleep(wait)
        
        return wrapper
    
    return decorator


# Configuration loading
def load_config(config_path=CONFIG_PATH, interface_config_path=INTERFACE_CONFIG_PATH) -> Tuple[Dict, Dict]:
    """
    Load configuration files
    
    Args:
        config_path (str): Path to main config file
        interface_config_path (str): Path to interface config file
        
    Returns:
        tuple: (config dict, interface config dict)
    """
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        with open(interface_config_path, 'r') as f:
            interface_config = json.load(f)
        
        return config, interface_config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        raise


# MongoDB connection
def connect_mongo(config: Dict) -> MongoClient:
    """
    Connect to MongoDB with configuration
    
    Args:
        config (dict): Configuration dictionary with MongoDB settings
        
    Returns:
        MongoClient: MongoDB client instance
    """
    try:
        mongo_config = config.get('mongodb', {})
        host = mongo_config.get('host', 'localhost')
        port = mongo_config.get('port', 27017)
        username = mongo_config.get('username', '')
        password = mongo_config.get('password', '')
        auth_db = mongo_config.get('authSource', 'admin')
        
        # Connection URI
        if username and password:
            mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{auth_db}"
        else:
            mongo_uri = f"mongodb://{host}:{port}/"
        
        # Connect with timeout
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        
        # Test connection
        client.admin.command('ping')
        logger.info(f"MongoDB connection successful")
        
        return client
    
    except ConnectionFailure as cf:
        logger.error(f"MongoDB connection failed: {str(cf)}")
        raise
    except Exception as e:
        logger.error(f"MongoDB error: {str(e)}")
        raise


# TuShare API request
@retry_with_backoff(max_retries=3, base_delay=1)
def call_tushare_api(interface_config: Dict, params: Dict, config: Dict) -> Dict:
    """
    Call TuShare API with parameters
    
    Args:
        interface_config (dict): Interface configuration
        params (dict): API parameters
        config (dict): Main configuration with API settings
        
    Returns:
        dict: API response
    """
    try:
        # Get API configuration
        api_config = config.get('tushare', {})
        token = api_config.get('token', '')
        url = api_config.get('url', 'http://api.tushare.pro')
        
        # Prepare the request
        req_params = {
            'api_name': interface_config.get('api_name', ''),
            'token': token,
            'params': params,
            'fields': interface_config.get('fields', '')
        }
        
        # Make the API call
        start_time = time.time()
        response = requests.post(url, json=req_params, timeout=60)
        end_time = time.time()
        duration = end_time - start_time
        
        # Check response
        if response.status_code != 200:
            logger.error(f"API request failed with status code: {response.status_code}")
            logger.error(f"Response: {response.text}")
            raise Exception(f"API request failed: {response.status_code}")
        
        # Parse response
        resp_data = response.json()
        
        # Check for API errors
        if resp_data.get('code') != 0:
            logger.error(f"API returned error: {resp_data}")
            raise Exception(f"API error: {resp_data.get('msg', 'Unknown API error')}")
        
        # Log performance
        data = resp_data.get('data', {})
        record_count = len(data.get('items', []))
        logger.info(f"API call successful: {record_count} records in {duration:.2f}s "  
                  f"({record_count/duration:.2f} records/sec)")
        
        return resp_data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error calling API: {str(e)}")
        raise


# Process TuShare API response
def process_trade_cal_response(response: Dict, exchange: str) -> List[Dict]:
    """
    Process trading calendar API response
    
    Args:
        response (dict): API response
        exchange (str): Exchange code
        
    Returns:
        list: Processed records
    """
    try:
        data = response.get('data', {})
        items = data.get('items', [])
        fields = data.get('fields', [])
        
        if not items or not fields:
            logger.warning(f"No data returned for exchange {exchange}")
            return []
        
        # Process records
        records = []
        for item in items:
            record = {fields[i]: val for i, val in enumerate(item)}
            
            # Add exchange if missing
            if 'exchange' not in record:
                record['exchange'] = exchange
                
            # Convert is_open to bool
            if 'is_open' in record:
                record['is_open'] = bool(record['is_open'])
            
            records.append(record)
        
        return records
    
    except Exception as e:
        logger.error(f"Error processing API response: {str(e)}")
        raise


# Load data into MongoDB
def store_in_mongodb(records: List[Dict], client: MongoClient, config: Dict, 
                    operation='upsert', chunk_size=None, performance_tracker=None) -> int:
    """
    Store records in MongoDB
    
    Args:
        records (list): Records to store
        client (MongoClient): MongoDB client
        config (dict): Configuration dictionary
        operation (str): Operation type ('insert' or 'upsert')
        chunk_size (int): Size of chunks for bulk operations
        performance_tracker: Performance tracking object
        
    Returns:
        int: Number of records processed
    """
    if not records:
        logger.warning("No records to store in MongoDB")
        return 0
    
    try:
        # Get MongoDB settings
        mongo_config = config.get('mongodb', {})
        db_name = mongo_config.get('database', 'quantdb')
        collection_name = mongo_config.get('collections', {}).get('trade_cal', 'trade_cal')
        
        # Get batch settings
        batch_settings = config.get('batch_settings', {})
        if chunk_size is None:
            chunk_size = batch_settings.get('mongo_chunk_size', 1000)
        
        # Get database and collection
        db = client[db_name]
        collection = db[collection_name]
        
        # Prepare bulk operations in chunks
        total_processed = 0
        start_time = time.time()
        
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            operations = []
            
            # Create bulk operations
            for record in chunk:
                if operation == 'upsert':
                    # Create unique key for upsert
                    filter_dict = {
                        'cal_date': record['cal_date'],
                        'exchange': record['exchange']
                    }
                    operations.append(UpdateOne(filter_dict, {'$set': record}, upsert=True))
                else:
                    operations.append(InsertOne(record))
            
            # Execute bulk operation
            chunk_start = time.time()
            result = collection.bulk_write(operations)
            chunk_end = time.time()
            chunk_duration = chunk_end - chunk_start
            
            # Track performance
            if performance_tracker:
                performance_tracker.track_mongodb_operation(
                    'bulk_write', len(chunk), chunk_duration, 
                    {'chunk_size': chunk_size, 'operation': operation}
                )
            
            # Update counts
            modified = result.modified_count + result.upserted_count
            total_processed += modified
            
            logger.info(f"MongoDB chunk {i//chunk_size + 1}: {modified} records in {chunk_duration:.2f}s " 
                      f"({len(chunk)/chunk_duration:.2f} records/sec)")
        
        # Log total performance
        end_time = time.time()
        total_duration = end_time - start_time
        if total_duration > 0:
            logger.info(f"Total MongoDB operation: {total_processed} records in {total_duration:.2f}s " 
                      f"({total_processed/total_duration:.2f} records/sec)")
        
        return total_processed
    
    except BulkWriteError as bwe:
        logger.error(f"Bulk write error: {bwe.details}")
        raise
    except Exception as e:
        logger.error(f"Error storing in MongoDB: {str(e)}")
        raise


# Fetch trading calendar data
def fetch_trade_cal(start_date: str, end_date: str, exchanges: List[str], 
                  config: Dict, interface_config: Dict, client: MongoClient, 
                  performance_tracker=None) -> List[Dict]:
    """
    Fetch trading calendar data for date range and exchanges
    
    Args:
        start_date (str): Start date (YYYYMMDD)
        end_date (str): End date (YYYYMMDD)
        exchanges (list): List of exchange codes
        config (dict): Configuration dictionary
        interface_config (dict): Interface configuration dictionary
        client (MongoClient): MongoDB client
        performance_tracker: Performance tracking object
        
    Returns:
        list: Fetched records
    """
    all_records = []
    
    for exchange in exchanges:
        # Prepare API parameters
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'exchange': exchange
        }
        
        # Call API and measure performance
        start_time = time.time()
        response = call_tushare_api(interface_config, params, config)
        records = process_trade_cal_response(response, exchange)
        end_time = time.time()
        duration = end_time - start_time
        
        # Track performance
        if performance_tracker:
            performance_tracker.track_api_call(
                exchange, start_date, end_date, len(records), duration
            )
        
        # Store in MongoDB
        if records:
            store_in_mongodb(records, client, config, 'upsert', 
                           performance_tracker=performance_tracker)
        
        all_records.extend(records)
    
    return all_records


# Process batch with performance tracking
def process_batch(batch: Dict, config: Dict, interface_config: Dict, 
                 client: MongoClient, performance_tracker=None) -> int:
    """
    Process a single batch
    
    Args:
        batch (dict): Batch definition
        config (dict): Configuration dictionary
        interface_config (dict): Interface configuration dictionary
        client (MongoClient): MongoDB client
        performance_tracker: Performance tracking object
        
    Returns:
        int: Number of records processed
    """
    exchanges = batch['exchanges']
    start_date = batch['start_date']
    end_date = batch['end_date']
    
    # Log batch start
    logger.info(f"Processing batch: {exchanges} from {start_date} to {end_date}")
    logger.info(f"Estimated records: ~{batch['est_records']:,}")
    
    # Process batch with performance tracking
    batch_start = time.time()
    all_records = fetch_trade_cal(
        start_date, end_date, exchanges, config, interface_config, client, performance_tracker
    )
    batch_end = time.time()
    batch_duration = batch_end - batch_start
    
    # Track batch performance
    if performance_tracker:
        performance_tracker.track_batch(
            exchanges, start_date, end_date, len(all_records), batch_duration
        )
    
    # Log batch completion
    record_count = len(all_records)
    logger.info(f"Batch completed: {record_count} records in {batch_duration:.2f}s " 
              f"({record_count/batch_duration:.2f} records/sec)")
    
    return record_count


# Process all batches
def process_all_batches(batches: List[Dict], config: Dict, interface_config: Dict, 
                      client: MongoClient, performance_tracker=None) -> int:
    """
    Process all batches sequentially
    
    Args:
        batches (list): List of batch definitions
        config (dict): Configuration dictionary
        interface_config (dict): Interface configuration dictionary
        client (MongoClient): MongoDB client
        performance_tracker: Performance tracking object
        
    Returns:
        int: Total number of records processed
    """
    total_records = 0
    total_batches = len(batches)
    
    for i, batch in enumerate(batches):
        # Log progress
        logger.info(f"Processing batch {i+1}/{total_batches} ({(i+1)/total_batches*100:.1f}%)")
        
        # Process batch
        record_count = process_batch(
            batch, config, interface_config, client, performance_tracker
        )
        
        total_records += record_count
        
        # Log progress
        logger.info(f"Progress: {i+1}/{total_batches} batches, {total_records:,} records processed")
    
    return total_records


# Verify data in MongoDB
def verify_mongodb_data(client: MongoClient, config: Dict, 
                      start_date: str = None, end_date: str = None, 
                      exchanges: List[str] = None) -> Dict:
    """
    Verify data in MongoDB
    
    Args:
        client (MongoClient): MongoDB client
        config (dict): Configuration dictionary
        start_date (str, optional): Start date for verification
        end_date (str, optional): End date for verification
        exchanges (list, optional): Exchanges to verify
        
    Returns:
        dict: Verification results
    """
    try:
        # Get MongoDB settings
        mongo_config = config.get('mongodb', {})
        db_name = mongo_config.get('database', 'quantdb')
        collection_name = mongo_config.get('collections', {}).get('trade_cal', 'trade_cal')
        
        # Get database and collection
        db = client[db_name]
        collection = db[collection_name]
        
        # Build query
        query = {}
        if start_date:
            if 'cal_date' not in query:
                query['cal_date'] = {}
            query['cal_date']['$gte'] = start_date
        if end_date:
            if 'cal_date' not in query:
                query['cal_date'] = {}
            query['cal_date']['$lte'] = end_date
        if exchanges:
            query['exchange'] = {'$in': exchanges}
        
        # Count total records
        total_count = collection.count_documents(query)
        
        # Count by exchange
        exchange_counts = {}
        if exchanges:
            for exchange in exchanges:
                exchange_query = dict(query)
                exchange_query['exchange'] = exchange
                exchange_counts[exchange] = collection.count_documents(exchange_query)
        
        # Count by is_open
        trading_days = 0
        non_trading_days = 0
        if 'is_open' in collection.find_one({}, {'is_open': 1, '_id': 0}) or {}:
            trading_query = dict(query)
            trading_query['is_open'] = True
            trading_days = collection.count_documents(trading_query)
            
            non_trading_query = dict(query)
            non_trading_query['is_open'] = False
            non_trading_days = collection.count_documents(non_trading_query)
        
        # Get date range
        date_range = {}
        if total_count > 0:
            earliest = collection.find(query).sort('cal_date', 1).limit(1)
            latest = collection.find(query).sort('cal_date', -1).limit(1)
            
            earliest_date = earliest[0]['cal_date'] if earliest.count() > 0 else None
            latest_date = latest[0]['cal_date'] if latest.count() > 0 else None
            date_range = {'earliest': earliest_date, 'latest': latest_date}
        
        # Return results
        results = {
            'total': total_count,
            'by_exchange': exchange_counts,
            'trading_days': trading_days,
            'non_trading_days': non_trading_days,
            'date_range': date_range
        }
        
        # Format numbers for logging
        logger.info("\nVerification Summary:")
        logger.info(f"Total records: {total_count:,}")
        logger.info(f"By exchange: {exchange_counts}")
        logger.info(f"Trading days: {trading_days}, Non-trading days: {non_trading_days}")
        if date_range.get('earliest') and date_range.get('latest'):
            logger.info(f"Date range: {date_range['earliest']} to {date_range['latest']}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error verifying MongoDB data: {str(e)}")
        raise


# Main function
def main():
    """
    Main function
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Fetch trading calendar data from TuShare API and store in MongoDB")
    parser.add_argument('--start-date', type=str, help="Start date (YYYYMMDD)")
    parser.add_argument('--end-date', type=str, help="End date (YYYYMMDD)")
    parser.add_argument('--exchange', type=str, help="Exchange code(s), comma-separated")
    parser.add_argument('--config', type=str, default=CONFIG_PATH, help="Path to config file")
    parser.add_argument('--years-per-batch', type=int, help="Override years per batch")
    parser.add_argument('--batch-size', type=int, help="Override batch size")
    parser.add_argument('--mongo-chunk-size', type=int, help="Override MongoDB chunk size")
    parser.add_argument('--batch-strategy', type=str, choices=['year', 'exchange', 'exchange_year'],
                       help="Override batch strategy")
    parser.add_argument('--verify-only', action='store_true', help="Only verify stored data")
    parser.add_argument('-v', '--verbose', action='store_true', help="Verbose logging")
    
    args = parser.parse_args()
    
    # Set log level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from {args.config}")
        config, interface_config = load_config(args.config, INTERFACE_CONFIG_PATH)
        
        # Apply command line overrides
        if args.years_per_batch:
            if 'batch_settings' not in config:
                config['batch_settings'] = {}
            config['batch_settings']['years_per_batch'] = args.years_per_batch
        
        if args.batch_size:
            if 'batch_settings' not in config:
                config['batch_settings'] = {}
            config['batch_settings']['max_batch_size'] = args.batch_size
        
        if args.mongo_chunk_size:
            if 'batch_settings' not in config:
                config['batch_settings'] = {}
            config['batch_settings']['mongo_chunk_size'] = args.mongo_chunk_size
        
        if args.batch_strategy:
            if 'batch_settings' not in config:
                config['batch_settings'] = {}
            config['batch_settings']['batch_strategy'] = args.batch_strategy
        
        # Parse date range
        start_date = args.start_date or '20200101'
        end_date = args.end_date or datetime.now().strftime('%Y%m%d')
        
        # Parse exchanges
        exchanges_str = args.exchange or 'SSE,SZSE'
        exchanges = exchanges_str.split(',')
        
        # Connect to MongoDB
        logger.info(f"Connecting to MongoDB at {config['mongodb']['host']}:{config['mongodb']['port']}")
        client = connect_mongo(config)
        
        # Verify-only mode
        if args.verify_only:
            logger.info("Verifying stored data (verify-only mode)")
            verify_mongodb_data(client, config, start_date, end_date, exchanges)
            return
        
        # Performance tracking
        performance_tracker = AdaptiveTracker()
        
        # Calculate optimal batch strategy if not specified
        batch_settings = config.get('batch_settings', {})
        if 'batch_strategy' not in batch_settings:
            optimal_strategy = determine_optimal_strategy(start_date, end_date, exchanges)
            config['batch_settings']['batch_strategy'] = optimal_strategy
            logger.info(f"Using automatically determined batch strategy: {optimal_strategy}")
        
        # Calculate intelligent batches
        logger.info("Calculating intelligent batches...")
        num_batches, est_records, batches = calculate_intelligent_batches(
            start_date, end_date, exchanges, config
        )
        
        logger.info(f"Processing {num_batches} batches for ~{est_records:,} records")
        
        # Process all batches
        total_processed = process_all_batches(
            batches, config, interface_config, client, performance_tracker
        )
        
        logger.info(f"Completed processing {total_processed:,} records across {num_batches} batches")
        
        # Generate performance report
        report = adaptive_report(performance_tracker, start_date, end_date)
        logger.info("Performance report generated.")
        
        # Verify final data
        logger.info("Verifying final data in MongoDB...")
        verify_mongodb_data(client, config, start_date, end_date, exchanges)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
