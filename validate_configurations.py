#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
validate_configurations.py

This module provides functions to validate MongoDB, Tushare API, and WAN
configurations using the ConfigStateManager. These functions are designed
to be called once to validate all configurations, instead of having each
task validate separately.
"""

import os
import sys
import json
import logging
import requests
import time
import importlib.util
from typing import Dict, List, Any, Tuple, Optional

# Import core components
from core.config_state_manager import config_state_manager
import yaml
from pathlib import Path

# Set up logging
logger = logging.getLogger("config_validator")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


def load_config() -> Dict[str, Any]:
    """
    Load configuration from the config file
    
    Returns:
        Dict[str, Any]: The loaded configuration
    """
    config_path = Path("config/config.yaml")
    if not config_path.exists():
        logger.error(f"Configuration file not found at {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
        
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def validate_mongodb_connection() -> Tuple[bool, Dict[str, Any], Optional[str]]:
    """
    Validate MongoDB connection
    
    Returns:
        Tuple containing:
            - Boolean indicating if validation was successful
            - Dict with connection details
            - Error message (if any)
    """
    logger.info("Validating MongoDB connection...")
    
    try:
        # Import MongoDB client
        from storage.mongodb_client import MongoDBClient
        from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
        
        # Load config
        config = load_config()
        mongo_config = config.get('mongodb', {})
        
        # Create MongoDB client
        mongo_client = MongoDBClient(
            host=mongo_config.get('host', 'localhost'),
            port=mongo_config.get('port', 27017),
            username=mongo_config.get('username', ''),
            password=mongo_config.get('password', ''),
            auth_db=mongo_config.get('auth_db', 'admin')
        )
        
        # Test connection
        mongo_client.client.admin.command('ping')
        
        # Get server info
        server_info = mongo_client.client.server_info()
        
        connection_info = {
            "host": mongo_config.get('host', 'localhost'),
            "port": mongo_config.get('port', 27017),
            "version": server_info.get('version', 'unknown'),
            "status": "connected"
        }
        
        logger.info(f"MongoDB validation successful: connected to {connection_info['host']}:{connection_info['port']}, "
                   f"version {connection_info['version']}")
        
        return True, connection_info, None
        
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        error_msg = f"Failed to connect to MongoDB: {str(e)}"
        logger.error(error_msg)
        return False, {}, error_msg
    except Exception as e:
        error_msg = f"Unexpected error validating MongoDB: {str(e)}"
        logger.error(error_msg)
        return False, {}, error_msg


def validate_tushare_connection() -> Tuple[bool, Dict[str, Any], Optional[str]]:
    """
    Validate Tushare API connection
    
    Returns:
        Tuple containing:
            - Boolean indicating if validation was successful
            - Dict with API info
            - Error message (if any)
    """
    logger.info("Validating Tushare API connection...")
    
    try:
        # Import Tushare client
        from data_fetcher.tushare_client import TushareClient
        
        # Load config
        config = load_config()
        tushare_config = config.get('tushare', {})
        
        # Create Tushare client
        ts_client = TushareClient(token=tushare_config.get('token', ''))
        
        # Test connection with a simple API call
        result = ts_client.trade_cal(exchange='', start_date='20230101', end_date='20230105')
        
        if result is not None and hasattr(result, 'shape') and result.shape[0] > 0:
            api_info = {
                "token": tushare_config.get('token', '')[:6] + '***',  # Mask most of the token
                "server": tushare_config.get('server', 'default'),
                "version": "api_v2",  # Tushare API version
                "status": "connected"
            }
            
            logger.info(f"Tushare API validation successful: connected to {api_info['server']} server")
            return True, api_info, None
        else:
            error_msg = "Tushare API returned empty or invalid result"
            logger.error(error_msg)
            return False, {}, error_msg
            
    except Exception as e:
        error_msg = f"Failed to connect to Tushare API: {str(e)}"
        logger.error(error_msg)
        return False, {}, error_msg


def validate_wan_configuration() -> Tuple[bool, List[Dict[str, Any]], Optional[str]]:
    """
    Validate WAN configuration
    
    Returns:
        Tuple containing:
            - Boolean indicating if validation was successful
            - List of dictionaries with interface details
            - Error message (if any)
    """
    logger.info("Validating WAN configuration...")
    
    try:
        # Load config
        config = load_config()
        wan_config = config.get('wan', {})
        
        # Check if WAN is enabled
        if not wan_config.get('enabled', False):
            logger.info("WAN configuration is disabled, skipping validation")
            return True, [], None
        
        # Get WAN interfaces
        wan_interfaces = wan_config.get('interfaces', [])
        if not wan_interfaces:
            error_msg = "No WAN interfaces defined in configuration"
            logger.warning(error_msg)
            return False, [], error_msg
        
        # Import port allocator
        from wan_manager.port_allocator import PortAllocator
        
        # Create port allocator
        port_allocator = PortAllocator(wan_config)
        
        # Test each interface by making a request through it
        validated_interfaces = []
        test_url = "http://httpbin.org/ip"
        seen_ips = set()
        
        for interface in wan_interfaces:
            interface_name = interface.get('name', 'unknown')
            port_range = interface.get('port_range', [0, 0])
            
            try:
                # Get a port from this interface's range
                local_port = port_allocator.get_port_for_interface(interface_name)
                
                if local_port is None:
                    logger.warning(f"Could not allocate port for interface {interface_name}, "
                                  f"port range {port_range}")
                    continue
                
                # Make a request through this interface
                source_address = ('0.0.0.0', local_port)
                response = requests.get(
                    test_url, 
                    timeout=10,
                    headers={'User-Agent': 'QuantDB-WAN-Validator/1.0'},
                    proxies=None
                )
                
                if response.status_code == 200:
                    response_data = response.json()
                    ip_address = response_data.get('origin', 'unknown')
                    
                    seen_ips.add(ip_address)
                    
                    interface_info = {
                        "name": interface_name,
                        "port_range": port_range,
                        "local_port_tested": local_port,
                        "detected_ip": ip_address,
                        "status": "connected"
                    }
                    validated_interfaces.append(interface_info)
                    logger.info(f"WAN interface {interface_name} validation successful: "
                               f"detected IP {ip_address}")
                else:
                    logger.warning(f"WAN interface {interface_name} returned status code "
                                  f"{response.status_code}")
            except Exception as e:
                logger.warning(f"Error testing WAN interface {interface_name}: {str(e)}")
        
        # Check if we have at least one validated interface
        if not validated_interfaces:
            error_msg = "No WAN interfaces could be validated"
            logger.error(error_msg)
            return False, [], error_msg
        
        # If we have multiple unique IPs, it's likely that we have multiple WAN interfaces
        is_multi_wan = len(seen_ips) > 1
        logger.info(f"Detected {'multiple' if is_multi_wan else 'single'} WAN environment "
                   f"with {len(validated_interfaces)} validated interfaces")
        
        return True, validated_interfaces, None
        
    except Exception as e:
        error_msg = f"Unexpected error validating WAN configuration: {str(e)}"
        logger.error(error_msg)
        return False, [], error_msg


def validate_all_configurations() -> Dict[str, Any]:
    """
    Validate all configurations (MongoDB, Tushare API, WAN)
    
    Returns:
        Dict with validation results
    """
    logger.info("Starting validation of all configurations...")
    
    # Validate MongoDB
    mongodb_success, mongodb_info, mongodb_error = validate_mongodb_connection()
    config_state_manager.set_mongodb_validated(mongodb_success, mongodb_info, mongodb_error)
    
    # Validate Tushare API
    tushare_success, tushare_info, tushare_error = validate_tushare_connection()
    config_state_manager.set_tushare_validated(tushare_success, tushare_info, tushare_error)
    
    # Validate WAN configuration
    wan_success, wan_interfaces, wan_error = validate_wan_configuration()
    config_state_manager.set_wan_validated(wan_success, wan_interfaces, wan_error)
    
    # Get summary of validation results
    validation_summary = config_state_manager.get_validation_summary()
    
    # Log summary
    logger.info(f"Configuration validation completed: MongoDB={validation_summary['mongodb']}, "
               f"Tushare={validation_summary['tushare']}, WAN={validation_summary['wan']}")
    
    return validation_summary


if __name__ == "__main__":
    # Set up more verbose logging for standalone execution
    logger.setLevel(logging.DEBUG)
    
    # Validate all configurations
    validation_results = validate_all_configurations()
    
    # Print validation summary
    print("\nValidation Summary:")
    print(json.dumps(validation_results, indent=2))
    
    # Exit with appropriate status code
    if validation_results['all_valid']:
        print("\nAll configurations validated successfully!")
        sys.exit(0)
    else:
        print("\nConfiguration validation failed. See above for details.")
        sys.exit(1)