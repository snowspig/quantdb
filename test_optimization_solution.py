#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trade Calendar Optimization Solution Test Script

This script performs a comprehensive test of the trade calendar optimization solution,
including testing different batch strategies and generating performance reports.
"""

import os
import sys
import time
import argparse
import logging
import subprocess
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('optimization_test.log')
    ]
)
logger = logging.getLogger(__name__)


def run_fetcher_with_strategy(start_date, end_date, exchanges, batch_strategy, years_per_batch=None):
    """
    Run the optimized fetcher with a specific strategy and parameters
    
    Args:
        start_date (str): Start date in YYYYMMDD format
        end_date (str): End date in YYYYMMDD format
        exchanges (str): Comma-separated list of exchanges
        batch_strategy (str): Batch strategy to use
        years_per_batch (int, optional): Years per batch
        
    Returns:
        tuple: (exit_code, execution_time)
    """
    try:
        command = [
            'python', 'ultimate_trade_calendar_fetcher.py',
            '--start-date', start_date,
            '--end-date', end_date,
            '--exchange', exchanges,
            '--batch-strategy', batch_strategy,
            '--verify-only'
        ]
        
        if years_per_batch is not None:
            command.extend(['--years-per-batch', str(years_per_batch)])
        
        logger.info(f"Running command: {' '.join(command)}")
        
        start_time = time.time()
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        execution_time = time.time() - start_time
        
        logger.info(f"Command completed in {execution_time:.2f} seconds with exit code {result.returncode}")
        
        return result.returncode, execution_time
    
    except Exception as e:
        logger.error(f"Error running fetcher: {str(e)}")
        return 1, 0


def test_batch_strategies(test_config):
    """
    Test different batch strategies and parameters
    
    Args:
        test_config (dict): Test configuration
        
    Returns:
        dict: Test results
    """
    results = {}
    
    for strategy in test_config['strategies']:
        for years in test_config['years_per_batch']:
            key = f"{strategy}_{years}"
            
            logger.info(f"\nTesting strategy: {strategy} with {years} years per batch")
            exit_code, execution_time = run_fetcher_with_strategy(
                test_config['start_date'],
                test_config['end_date'],
                test_config['exchanges'],
                strategy,
                years
            )
            
            results[key] = {
                'strategy': strategy,
                'years_per_batch': years,
                'exit_code': exit_code,
                'execution_time': execution_time
            }
    
    return results


def monitor_performance():
    """
    Run the performance monitoring script
    
    Returns:
        int: Exit code
    """
    try:
        command = [
            'python', 'monitor_trade_calendar_performance.py',
            '--html-report',
            '--export-json'
        ]
        
        logger.info(f"Running performance monitoring: {' '.join(command)}")
        
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        logger.info(f"Performance monitoring completed with exit code {result.returncode}")
        return result.returncode
    
    except Exception as e:
        logger.error(f"Error running performance monitoring: {str(e)}")
        return 1


def verify_optimization():
    """
    Verify the optimization by running the fetcher with the recommended settings
    
    Returns:
        int: Exit code
    """
    try:
        # Get current year
        current_year = datetime.now().year
        
        # Calculate last 3 years
        start_date = f"{current_year - 3}0101"
        end_date = datetime.now().strftime('%Y%m%d')
        
        logger.info(f"Verifying optimization for date range: {start_date} to {end_date}")
        
        # Use the recommended settings from our performance testing
        exit_code, execution_time = run_fetcher_with_strategy(
            start_date,
            end_date,
            'SSE,SZSE',
            'exchange_year',
            5
        )
        
        if exit_code == 0:
            logger.info(f"Verification successful in {execution_time:.2f} seconds")
        else:
            logger.error("Verification failed")
        
        return exit_code
    
    except Exception as e:
        logger.error(f"Error verifying optimization: {str(e)}")
        return 1


def analyze_results(results):
    """
    Analyze test results and determine the best strategy
    
    Args:
        results (dict): Test results
        
    Returns:
        dict: Analysis results
    """
    successful_tests = {k: v for k, v in results.items() if v['exit_code'] == 0}
    
    if not successful_tests:
        return {'best_strategy': None, 'best_years': None, 'message': "No successful tests"}
    
    # Find fastest execution
    fastest_key = min(successful_tests, key=lambda k: successful_tests[k]['execution_time'])
    fastest = successful_tests[fastest_key]
    
    # Calculate average execution time by strategy
    strategy_times = {}
    for k, v in successful_tests.items():
        strategy = v['strategy']
        if strategy not in strategy_times:
            strategy_times[strategy] = []
        strategy_times[strategy].append(v['execution_time'])
    
    avg_times = {s: sum(times)/len(times) for s, times in strategy_times.items()}
    best_strategy = min(avg_times, key=avg_times.get)
    
    # Calculate average execution time by years per batch
    years_times = {}
    for k, v in successful_tests.items():
        years = v['years_per_batch']
        if years not in years_times:
            years_times[years] = []
        years_times[years].append(v['execution_time'])
    
    avg_year_times = {y: sum(times)/len(times) for y, times in years_times.items()}
    best_years = min(avg_year_times, key=avg_year_times.get)
    
    return {
        'best_strategy': fastest['strategy'],
        'best_years': fastest['years_per_batch'],
        'fastest_execution': fastest['execution_time'],
        'avg_times_by_strategy': avg_times,
        'avg_times_by_years': avg_year_times,
        'all_results': successful_tests
    }


def generate_report(analysis_results):
    """
    Generate a report of test results
    
    Args:
        analysis_results (dict): Analysis results
        
    Returns:
        str: Report content
    """
    lines = [
        "===== OPTIMIZATION TEST REPORT =====",
        f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ""
    ]
    
    if analysis_results['best_strategy'] is None:
        lines.append("No successful tests to analyze")
        return "\n".join(lines)
    
    lines.extend([
        "Best Strategy Configuration:",
        f"- Strategy: {analysis_results['best_strategy']}",
        f"- Years per batch: {analysis_results['best_years']}",
        f"- Execution time: {analysis_results['fastest_execution']:.2f} seconds",
        ""
    ])
    
    lines.append("Performance by Strategy:")
    for strategy, time in analysis_results['avg_times_by_strategy'].items():
        lines.append(f"- {strategy}: {time:.2f} seconds")
    
    lines.append("\nPerformance by Years per Batch:")
    for years, time in analysis_results['avg_times_by_years'].items():
        lines.append(f"- {years} years: {time:.2f} seconds")
    
    lines.append("\nDetailed Results:")
    for key, result in analysis_results['all_results'].items():
        lines.append(f"- {result['strategy']} with {result['years_per_batch']} years: {result['execution_time']:.2f} seconds")
    
    report = "\n".join(lines)
    
    # Save report to file
    with open("optimization_test_report.txt", "w") as f:
        f.write(report)
    
    logger.info("Test report saved to optimization_test_report.txt")
    
    return report


def main():
    """
    Main function
    """
    parser = argparse.ArgumentParser(description="Test the trade calendar optimization solution")
    parser.add_argument('--quick-test', action='store_true', help="Run a quick test only")
    parser.add_argument('--verify-only', action='store_true', help="Only verify the optimization")
    
    args = parser.parse_args()
    
    try:
        # Create test directory
        os.makedirs("test_results", exist_ok=True)
        
        if args.verify_only:
            # Just verify the optimization
            exit_code = verify_optimization()
            sys.exit(exit_code)
        
        if args.quick_test:
            # Run a quick test with limited scope
            test_config = {
                'start_date': '20230101',
                'end_date': '20231231',
                'exchanges': 'SSE,SZSE',
                'strategies': ['exchange_year'],
                'years_per_batch': [5]
            }
        else:
            # Run comprehensive test
            test_config = {
                'start_date': '20200101',
                'end_date': '20231231',
                'exchanges': 'SSE,SZSE',
                'strategies': ['year', 'exchange', 'exchange_year'],
                'years_per_batch': [3, 5, 10]
            }
        
        logger.info("Starting optimization tests...")
        results = test_batch_strategies(test_config)
        
        # Analyze results
        analysis = analyze_results(results)
        report = generate_report(analysis)
        print(report)
        
        # Run performance monitoring
        logger.info("Running performance monitoring...")
        monitor_performance()
        
        # Verify final optimization
        logger.info("Verifying final optimization...")
        verify_optimization()
        
        logger.info("All tests completed successfully")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
