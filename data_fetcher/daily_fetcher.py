#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import logging
import argparse
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
import pandas as pd
from loguru import logger

# Add parent directory to path to import modules from other directories
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from data_fetcher.tushare_client import tushare_client
from storage.mongodb_client import mongodb_client
from config import config_manager

class DailyFetcher:
    """
    Class for fetching daily stock data from Tushare API and storing it in MongoDB.
    Supports different modes: recent (default), full, and date range.
    Implements batch processing to handle large data volumes.
    """
    
    def __init__(self, mock=False, verbose=False):
        """
        Initialize DailyFetcher with configuration and clients.
        
        Args:
            mock (bool): Whether to run in mock mode (without actual API calls)
            verbose (bool): Whether to output verbose logs
        """
        self.mock = mock
        self.verbose = verbose
        
        if verbose:
            logger.level("INFO")
        
        # Load configuration
        if not mock:
            self.collection = mongodb_client.get_collection('daily')
            
            # Create indices for faster queries if they don't exist
            existing_indices = self.collection.index_information()
            if not any('ts_code_1_trade_date_-1' in idx for idx in existing_indices.keys()):
                logger.info("Creating index on (ts_code, trade_date)")
                self.collection.create_index(
                    [("ts_code", 1), ("trade_date", -1)], 
                    unique=True, 
                    background=True
                )
    
    def _get_trade_cal(self, start_date, end_date):
        """
        Get the trading calendar for the specified date range.
        
        Args:
            start_date (str): Start date in format 'YYYYMMDD'
            end_date (str): End date in format 'YYYYMMDD'
            
        Returns:
            list: List of trading dates
        """
        try:
            params = {
                'start_date': start_date,
                'end_date': end_date,
                'is_open': '1'  # 1 indicates trading days
            }
            trade_cal = tushare_client.get_data('trade_cal', params=params)
            if trade_cal is not None and not trade_cal.empty:
                return trade_cal['cal_date'].tolist()
            return []
        except Exception as e:
            logger.error(f"Error fetching trading calendar: {e}")
            return []
    
    def _get_stock_list(self):
        """
        Get a list of all stock codes from Tushare.
        
        Returns:
            list: List of stock codes
        """
        try:
            stock_basic = tushare_client.get_data('stock_basic')
            if stock_basic is not None and not stock_basic.empty:
                return stock_basic['ts_code'].tolist()
            return []
        except Exception as e:
            logger.error(f"Error fetching stock list: {e}")
            return []
    
    def _fetch_daily_batch(self, start_date, end_date, ts_code=None, limit=None, offset=0):
        """
        Fetch a batch of daily stock data.
        
        Args:
            start_date (str): Start date in format 'YYYYMMDD'
            end_date (str): End date in format 'YYYYMMDD'
            ts_code (str, optional): Stock code(s) to fetch. Can be comma-separated list.
            limit (int, optional): Maximum number of records to fetch
            offset (int, optional): Offset for pagination
            
        Returns:
            pandas.DataFrame: Daily stock data
        """
        try:
            params = {
                'start_date': start_date,
                'end_date': end_date
            }
            
            if ts_code:
                params['ts_code'] = ts_code
                
            if limit:
                params['limit'] = limit
                params['offset'] = offset
                
            logger.info(f"Fetching daily data with params: {params}")
            return tushare_client.get_data('daily', params=params)
        except Exception as e:
            logger.error(f"Error fetching daily data: {e}")
            return None
    
    def _save_to_mongodb(self, data):
        """
        Save data to MongoDB.
        
        Args:
            data (pandas.DataFrame): Data to save
            
        Returns:
            int: Number of documents inserted
        """
        if data is None or data.empty:
            logger.warning("No data to save to MongoDB")
            return 0
        
        try:
            # Convert DataFrame to list of dictionaries
            data_dict = data.to_dict('records')
            
            # Insert data into MongoDB with upsert to avoid duplicates
            inserted_count = 0
            for record in data_dict:
                result = self.collection.update_one(
                    {
                        'ts_code': record['ts_code'],
                        'trade_date': record['trade_date']
                    },
                    {'$set': record},
                    upsert=True
                )
                if result.upserted_id or result.modified_count > 0:
                    inserted_count += 1
            
            logger.info(f"Inserted or updated {inserted_count} documents out of {len(data_dict)} in MongoDB")
            return inserted_count
        except Exception as e:
            logger.error(f"Error saving data to MongoDB: {e}")
            return 0
    
    def fetch_by_date_range(self, start_date, end_date, batch_size=5000):
        """
        Fetch daily stock data for a specific date range.
        
        Args:
            start_date (str): Start date in format 'YYYYMMDD'
            end_date (str): End date in format 'YYYYMMDD'
            batch_size (int): Size of each batch
            
        Returns:
            int: Total number of documents inserted
        """
        if self.mock:
            logger.info(f"Mock mode: Would fetch daily data from {start_date} to {end_date}")
            return 0
        
        # Check if the date range contains many trading days
        trading_days = self._get_trade_cal(start_date, end_date)
        total_days = len(trading_days)
        
        # If potentially more than 10,000 records, fetch by stock code in batches
        if total_days > 20:  # Each stock has a record per trading day, so with 5 stocks we'd hit 10,000
            logger.info(f"Date range contains {total_days} trading days, fetching by stock code batches")
            stock_list = self._get_stock_list()
            total_inserted = 0
            
            # Process stocks in batches to reduce API calls
            stock_batches = [stock_list[i:i + 50] for i in range(0, len(stock_list), 50)]
            for batch_idx, stock_batch in enumerate(stock_batches):
                logger.info(f"Processing stock batch {batch_idx + 1}/{len(stock_batches)}")
                # Join stock codes with comma for batch API call
                ts_codes = ','.join(stock_batch)
                
                # Fetch data for this batch of stocks
                data = self._fetch_daily_batch(start_date, end_date, ts_code=ts_codes)
                inserted = self._save_to_mongodb(data)
                total_inserted += inserted
                
                # Rate limiting to avoid API restrictions
                time.sleep(1)
                
            return total_inserted
        else:
            # If the date range is manageable, fetch in batches by date
            logger.info(f"Date range contains {total_days} trading days, fetching in batches")
            total_inserted = 0
            offset = 0
            
            while True:
                data = self._fetch_daily_batch(
                    start_date, 
                    end_date, 
                    limit=batch_size, 
                    offset=offset
                )
                
                if data is None or data.empty:
                    break
                    
                inserted = self._save_to_mongodb(data)
                total_inserted += inserted
                
                if len(data) < batch_size:
                    break
                    
                offset += batch_size
                time.sleep(1)  # Rate limiting
                
            return total_inserted
    
    def fetch_recent(self, days=7):
        """
        Fetch daily stock data for the last specified number of days.
        
        Args:
            days (int): Number of days to fetch
            
        Returns:
            int: Total number of documents inserted
        """
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y%m%d')
        
        logger.info(f"Fetching recent daily data from {start_date} to {end_date}")
        return self.fetch_by_date_range(start_date, end_date)
    
    def fetch_full(self):
        """
        Fetch all available daily stock data.
        
        Returns:
            int: Total number of documents inserted
        """
        # Use a very early start date to get all data
        start_date = '19900101'
        end_date = datetime.datetime.now().strftime('%Y%m%d')
        
        logger.info(f"Fetching full daily data from {start_date} to {end_date}")
        return self.fetch_by_date_range(start_date, end_date)


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Fetch daily stock data from Tushare API')
    
    # Mode selection arguments (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--recent', action='store_true', default=True,
                          help='Fetch data for the last week (default)')
    mode_group.add_argument('--full', action='store_true',
                          help='Fetch all historical data')
    
    # Date range arguments
    parser.add_argument('--start-date', type=str,
                       help='Start date in format YYYYMMDD')
    parser.add_argument('--end-date', type=str,
                       help='End date in format YYYYMMDD')
    
    # Other options
    parser.add_argument('--days', type=int, default=7,
                       help='Number of days to fetch when using --recent')
    parser.add_argument('--batch-size', type=int, default=5000,
                       help='Batch size for API requests')
    parser.add_argument('--mock', action='store_true',
                       help='Run in mock mode without actual API calls')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    
    return parser.parse_args()


def main():
    """
    Main function to run the daily data fetcher.
    """
    args = parse_arguments()
    
    try:
        logger.info("Starting daily_fetcher.py")
        fetcher = DailyFetcher(mock=args.mock, verbose=args.verbose)
        
        if args.full:
            logger.info("Starting full historical data fetch")
            fetcher.fetch_full()
        elif args.start_date and args.end_date:
            logger.info(f"Starting data fetch for date range {args.start_date} to {args.end_date}")
            fetcher.fetch_by_date_range(args.start_date, args.end_date, batch_size=args.batch_size)
        else:  # Default to recent mode
            logger.info(f"Starting recent data fetch (last {args.days} days)")
            fetcher.fetch_recent(days=args.days)
            
        logger.info("Daily data fetch completed successfully")
        
    except Exception as e:
        logger.exception(f"Error in main: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()