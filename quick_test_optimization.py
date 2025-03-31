#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick Test for Trade Calendar Optimization

This script performs a quick verification test of the trade calendar optimization solution
without actually fetching data from the API. It validates that the intelligent batch
processing logic is working correctly by calculating batches for different scenarios.
"""

import argparse
import logging
import sys
import yaml
import json
from datetime import datetime
from pprint import pprint

# Import intelligent batch processing
import intelligent_batch_processing
from adaptive_batch import determine_optimal_strategy, estimate_records_in_range

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default config path
CONFIG_PATH = "config/config.yaml"


def load_config(config_path=CONFIG_PATH):
    """Load configuration from file"""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        return {}


def test_batch_calculation(start_date, end_date, exchanges, batch_strategy=None, years_per_batch=None):
    """Test batch calculation logic"""
    try:
        # Load config
        config = load_config()
        
        # Override settings if provided
        if 'batch_settings' not in config:
            config['batch_settings'] = {}
        
        if batch_strategy:
            config['batch_settings']['batch_strategy'] = batch_strategy
        
        if years_per_batch:
            config['batch_settings']['years_per_batch'] = years_per_batch
        
        # Calculate intelligent batches
        logger.info(f"Calculating batches for {start_date} to {end_date} with exchanges {exchanges}")
        
        # Determine optimal strategy if not specified
        if not batch_strategy:
            optimal_strategy = determine_optimal_strategy(start_date, end_date, exchanges)
            config['batch_settings']['batch_strategy'] = optimal_strategy
            logger.info(f"Using automatically determined batch strategy: {optimal_strategy}")
        
        # Calculate batches
        num_batches, est_records, batches = intelligent_batch_processing.calculate_intelligent_batches(
            start_date, end_date, exchanges, config
        )
        
        # Return results
        return {
            'num_batches': num_batches,
            'est_records': est_records,
            'batches': batches,
            'config': config['batch_settings']
        }
        
    except Exception as e:
        logger.error(f"Error in batch calculation: {str(e)}")
        return {'error': str(e)}


def print_batch_summary(result):
    """Print batch calculation summary"""
    if 'error' in result:
        logger.error(f"Batch calculation failed: {result['error']}")
        return
    
    print("\n===== BATCH CALCULATION SUMMARY =====\n")
    print(f"Configuration:")
    print(f"  - Strategy: {result['config'].get('batch_strategy', 'N/A')}")
    print(f"  - Years per batch: {result['config'].get('years_per_batch', 'N/A')}")
    print(f"  - Max batch size: {result['config'].get('max_batch_size', 'N/A')}")
    
    print(f"\nResults:")
    print(f"  - Total batches: {result['num_batches']}")
    print(f"  - Estimated records: {result['est_records']:,}")
    
    print("\nBatch Details:")
    for i, batch in enumerate(result['batches']):
        print(f"  Batch {i+1}:")
        print(f"    - Exchanges: {', '.join(batch['exchanges'])}")
        print(f"    - Date range: {batch['start_date']} to {batch['end_date']}")
        print(f"    - Estimated records: {batch['est_records']:,}")
        print(f"    - Years: {batch['years']}")
        print(f"    - Strategy: {batch['strategy']}")


def export_batch_config(result, output_file):
    """Export batch configuration to JSON"""
    try:
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        logger.info(f"Batch configuration exported to {output_file}")
    except Exception as e:
        logger.error(f"Error exporting batch configuration: {str(e)}")


def run_test_scenarios():
    """Run multiple test scenarios"""
    scenarios = [
        {
            'name': "Small range (1 year)",
            'start_date': "20230101",
            'end_date': "20231231",
            'exchanges': ["SSE", "SZSE"],
            'expected_strategy': "single_batch"
        },
        {
            'name': "Medium range (5 years)",
            'start_date': "20190101",
            'end_date': "20231231",
            'exchanges': ["SSE", "SZSE"],
            'expected_strategy': "year"
        },
        {
            'name': "Large range (30+ years)",
            'start_date': "19900101",
            'end_date': "20240101",
            'exchanges': ["SSE", "SZSE", "SHN", "SZN"],
            'expected_strategy': "exchange_year"
        }
    ]
    
    results = []
    
    for scenario in scenarios:
        logger.info(f"\nRunning test scenario: {scenario['name']}")
        result = test_batch_calculation(
            scenario['start_date'],
            scenario['end_date'],
            scenario['exchanges']
        )
        
        # Add scenario info to result
        if 'error' not in result:
            result['scenario'] = scenario['name']
            result['expected_strategy'] = scenario['expected_strategy']
            result['actual_strategy'] = result['config'].get('batch_strategy')
            result['passed'] = result['actual_strategy'] == scenario['expected_strategy']
        
        results.append(result)
        print_batch_summary(result)
    
    # Summary of all scenarios
    print("\n===== TEST SCENARIOS SUMMARY =====\n")
    for result in results:
        if 'error' in result:
            print(f"Scenario failed with error: {result['error']}")
        else:
            status = "PASSED" if result['passed'] else "FAILED"
            print(f"{result['scenario']}: {status} (Expected: {result['expected_strategy']}, Got: {result['actual_strategy']})")
    
    return results


def main():
    """
    Main function
    """
    parser = argparse.ArgumentParser(description="Quick test for trade calendar optimization")
    parser.add_argument('--start-date', type=str, help="Start date (YYYYMMDD)")
    parser.add_argument('--end-date', type=str, help="End date (YYYYMMDD)")
    parser.add_argument('--exchange', type=str, help="Exchange codes (comma-separated)")
    parser.add_argument('--strategy', type=str, choices=['year', 'exchange', 'exchange_year', 'single_batch'], 
                      help="Batch strategy")
    parser.add_argument('--years-per-batch', type=int, help="Years per batch")
    parser.add_argument('--run-scenarios', action='store_true', help="Run test scenarios")
    parser.add_argument('--output', type=str, help="Output file for batch configuration")
    
    args = parser.parse_args()
    
    try:
        if args.run_scenarios:
            # Run test scenarios
            results = run_test_scenarios()
            
            # Export results if output file specified
            if args.output:
                export_batch_config(results, args.output)
            
            return
        
        # Get parameters
        start_date = args.start_date or "20200101"
        end_date = args.end_date or datetime.now().strftime('%Y%m%d')
        
        exchanges_str = args.exchange or "SSE,SZSE"
        exchanges = exchanges_str.split(',')
        
        # Calculate batches
        result = test_batch_calculation(
            start_date, 
            end_date, 
            exchanges, 
            args.strategy, 
            args.years_per_batch
        )
        
        # Print results
        print_batch_summary(result)
        
        # Export results if output file specified
        if args.output:
            export_batch_config(result, args.output)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
