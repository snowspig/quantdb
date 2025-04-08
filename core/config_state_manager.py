#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Configuration State Manager - Tracks and manages validation state of various configurations

This module provides a singleton class that keeps track of whether MongoDB, Tushare API,
and WAN configurations have been validated successfully. It allows components to check
if validation has already been performed, avoiding redundant validation operations when
multiple scripts are run through gorepe.py.
"""

import os
import json
import logging
import time
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path


class ConfigStateManager:
    """
    Singleton class to manage configuration validation state
    
    This class stores the results of connection and configuration tests to avoid
    redundant validation checks when multiple components are initialized.
    """
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigStateManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        # Set up logging
        self.logger = logging.getLogger("ConfigStateManager")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # State storage
        self.state = {
            "mongodb": {
                "validated": False,
                "timestamp": None,
                "connection_info": None,
                "error": None
            },
            "tushare": {
                "validated": False,
                "timestamp": None,
                "api_info": None,
                "error": None
            },
            "wan": {
                "validated": False,
                "timestamp": None,
                "interfaces": [],
                "error": None
            }
        }
        
        # Configuration for state persistence
        self.state_file_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            "config", 
            "config_state.json"
        )
        
        # Load existing state if available
        self._load_state()
        
        # State timeout (in seconds) - validation will expire after this time
        self.validation_timeout = 3600  # 1 hour
        
        self._initialized = True
        
        self.logger.info("Configuration State Manager initialized")
    
    def _load_state(self) -> None:
        """Load persisted state from file if available"""
        try:
            if os.path.exists(self.state_file_path):
                with open(self.state_file_path, 'r') as f:
                    saved_state = json.load(f)
                    self.state.update(saved_state)
                    self.logger.info(f"Loaded saved configuration state from {self.state_file_path}")
        except Exception as e:
            self.logger.warning(f"Failed to load configuration state: {str(e)}")
    
    def save_state(self) -> None:
        """Save current state to file"""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.state_file_path), exist_ok=True)
            
            with open(self.state_file_path, 'w') as f:
                json.dump(self.state, f, indent=2)
                self.logger.info(f"Saved configuration state to {self.state_file_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save configuration state: {str(e)}")
    
    def _is_validation_expired(self, timestamp: Optional[float]) -> bool:
        """Check if validation timestamp has expired"""
        if timestamp is None:
            return True
        current_time = time.time()
        return (current_time - timestamp) > self.validation_timeout
    
    def set_mongodb_validated(self, success: bool, connection_info: Dict[str, Any] = None, error: str = None) -> None:
        """
        Set MongoDB validation state
        
        Args:
            success: Whether validation was successful
            connection_info: Information about the MongoDB connection
            error: Error message if validation failed
        """
        self.state["mongodb"]["validated"] = success
        self.state["mongodb"]["timestamp"] = time.time() if success else None
        self.state["mongodb"]["connection_info"] = connection_info
        self.state["mongodb"]["error"] = error
        self.logger.info(f"MongoDB validation {'succeeded' if success else 'failed'}")
        self.save_state()
    
    def set_tushare_validated(self, success: bool, api_info: Dict[str, Any] = None, error: str = None) -> None:
        """
        Set Tushare API validation state
        
        Args:
            success: Whether validation was successful
            api_info: Information about the Tushare API
            error: Error message if validation failed
        """
        self.state["tushare"]["validated"] = success
        self.state["tushare"]["timestamp"] = time.time() if success else None
        self.state["tushare"]["api_info"] = api_info
        self.state["tushare"]["error"] = error
        self.logger.info(f"Tushare API validation {'succeeded' if success else 'failed'}")
        self.save_state()
    
    def set_wan_validated(self, success: bool, interfaces: List[Dict[str, Any]] = None, error: str = None) -> None:
        """
        Set WAN configuration validation state
        
        Args:
            success: Whether validation was successful
            interfaces: Information about the WAN interfaces
            error: Error message if validation failed
        """
        self.state["wan"]["validated"] = success
        self.state["wan"]["timestamp"] = time.time() if success else None
        self.state["wan"]["interfaces"] = interfaces or []
        self.state["wan"]["error"] = error
        self.logger.info(f"WAN configuration validation {'succeeded' if success else 'failed'}")
        self.save_state()
    
    def is_mongodb_validated(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if MongoDB configuration has been validated
        
        Returns:
            Tuple containing:
                - Boolean indicating if the validation is current and successful
                - Dict with validation details
        """
        mongodb_state = self.state["mongodb"]
        is_valid = (
            mongodb_state["validated"] and 
            not self._is_validation_expired(mongodb_state["timestamp"])
        )
        
        return is_valid, {
            "validated": mongodb_state["validated"],
            "timestamp": mongodb_state["timestamp"],
            "connection_info": mongodb_state["connection_info"],
            "error": mongodb_state["error"],
            "is_current": not self._is_validation_expired(mongodb_state["timestamp"]) if mongodb_state["timestamp"] else False
        }
    
    def is_tushare_validated(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if Tushare API configuration has been validated
        
        Returns:
            Tuple containing:
                - Boolean indicating if the validation is current and successful
                - Dict with validation details
        """
        tushare_state = self.state["tushare"]
        is_valid = (
            tushare_state["validated"] and 
            not self._is_validation_expired(tushare_state["timestamp"])
        )
        
        return is_valid, {
            "validated": tushare_state["validated"],
            "timestamp": tushare_state["timestamp"],
            "api_info": tushare_state["api_info"],
            "error": tushare_state["error"],
            "is_current": not self._is_validation_expired(tushare_state["timestamp"]) if tushare_state["timestamp"] else False
        }
    
    def is_wan_validated(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if WAN configuration has been validated
        
        Returns:
            Tuple containing:
                - Boolean indicating if the validation is current and successful
                - Dict with validation details
        """
        wan_state = self.state["wan"]
        is_valid = (
            wan_state["validated"] and 
            not self._is_validation_expired(wan_state["timestamp"])
        )
        
        return is_valid, {
            "validated": wan_state["validated"],
            "timestamp": wan_state["timestamp"],
            "interfaces": wan_state["interfaces"],
            "error": wan_state["error"],
            "is_current": not self._is_validation_expired(wan_state["timestamp"]) if wan_state["timestamp"] else False
        }
    
    def reset_validations(self) -> None:
        """Reset all validation states to force revalidation"""
        for component in self.state:
            self.state[component]["validated"] = False
            self.state[component]["timestamp"] = None
        self.logger.info("All configuration validations have been reset")
        self.save_state()
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all validation states
        
        Returns:
            Dict with validation summary for all components
        """
        mongodb_valid, mongodb_details = self.is_mongodb_validated()
        tushare_valid, tushare_details = self.is_tushare_validated()
        wan_valid, wan_details = self.is_wan_validated()
        
        return {
            "all_valid": mongodb_valid and tushare_valid and wan_valid,
            "mongodb": mongodb_valid,
            "tushare": tushare_valid,
            "wan": wan_valid,
            "details": {
                "mongodb": mongodb_details,
                "tushare": tushare_details,
                "wan": wan_details
            }
        }


# Create singleton instance
config_state_manager = ConfigStateManager()

if __name__ == "__main__":
    # Simple test code
    print("Testing Configuration State Manager")
    
    # Get the manager instance
    manager = ConfigStateManager()
    
    # Check initial state
    print("\nInitial state:")
    print(json.dumps(manager.get_validation_summary(), indent=2))
    
    # Set some validation states
    print("\nSetting validation states...")
    manager.set_mongodb_validated(True, {"host": "localhost", "port": 27017})
    manager.set_tushare_validated(True, {"api_name": "tushare", "version": "1.0"})
    manager.set_wan_validated(True, [{"name": "wan1", "port_range": [50001, 51000]}])
    
    # Check updated state
    print("\nUpdated state:")
    print(json.dumps(manager.get_validation_summary(), indent=2))
    
    # Reset validations
    print("\nResetting validations...")
    manager.reset_validations()
    
    # Check final state
    print("\nFinal state:")
    print(json.dumps(manager.get_validation_summary(), indent=2))