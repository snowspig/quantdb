#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trade Calendar System Integration Test

This script performs a complete integration test of all components of the
trade calendar optimization solution, including fetching, monitoring,
and performance analysis.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime, timedelta
import subprocess

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('integration_test.log')
    ]
)
logger = logging.getLogger(__name__)

# Test directory
TEST_DIR = "integration_test_results"

# Component scripts
ULTIMATE_FETCHER = "ultimate_trade_calendar_fetcher.py"
MONITOR_PERFORMANCE = "monitor_trade_calendar_performance.py"
QUICK_TEST = "quick_test_optimization.py"


def run_command(command, description=None):
    """
    Run a system command and return the result
    
    Args:
        command (list): Command and arguments
        description (str, optional): Description of the command
        
    Returns:
        tuple: (success, stdout, stderr, duration)
    """
    if description:
        logger.info(f"Running: {description}")
    logger.info(f"Command: {' '.join(command)}")
    
    try:
        start_time = time.time()
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        duration = time.time() - start_time
        success = result.returncode == 0
        
        if success:
            logger.info(f"Command completed successfully in {duration:.2f} seconds")
        else:
            logger.error(f"Command failed with exit code {result.returncode}")
            if result.stderr:
                logger.error(f"Error: {result.stderr}")
        
        return success, result.stdout, result.stderr, duration
    
    except Exception as e:
        logger.error(f"Exception running command: {str(e)}")
        return False, "", str(e), 0


def ensure_test_directory():
    """
    Ensure test directory exists
    
    Returns:
        str: Path to test directory
    """
    os.makedirs(TEST_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    test_run_dir = os.path.join(TEST_DIR, f"test_run_{timestamp}")
    os.makedirs(test_run_dir, exist_ok=True)
    return test_run_dir


def save_test_result(test_run_dir, test_name, success, stdout, stderr, duration, additional_data=None):
    """
    Save test result to file
    
    Args:
        test_run_dir (str): Test run directory
        test_name (str): Test name
        success (bool): Success flag
        stdout (str): Command stdout
        stderr (str): Command stderr
        duration (float): Test duration
        additional_data (dict, optional): Additional data to save
    """
    result = {
        "test_name": test_name,
        "success": success,
        "duration": duration,
        "timestamp": datetime.now().isoformat(),
        "stdout": stdout,
        "stderr": stderr
    }
    
    if additional_data:
        result.update(additional_data)
    
    # Save to JSON file
    result_file = os.path.join(test_run_dir, f"{test_name}.json")
    with open(result_file, "w") as f:
        json.dump(result, f, indent=2)
    
    # Also save stdout and stderr to separate files for easier viewing
    if stdout:
        with open(os.path.join(test_run_dir, f"{test_name}_stdout.txt"), "w") as f:
            f.write(stdout)
    
    if stderr:
        with open(os.path.join(test_run_dir, f"{test_name}_stderr.txt"), "w") as f:
            f.write(stderr)
    
    logger.info(f"Test result saved to {result_file}")


def test_batch_calculation(test_run_dir):
    """
    Test batch calculation functionality
    
    Args:
        test_run_dir (str): Test run directory
        
    Returns:
        bool: Success flag
    """
    test_name = "batch_calculation"
    logger.info(f"Running {test_name} test")
    
    command = ["python", QUICK_TEST, "--run-scenarios", "--output", 
              os.path.join(test_run_dir, "batch_calculation_results.json")]
    
    success, stdout, stderr, duration = run_command(
        command, "Batch calculation test"
    )
    
    save_test_result(test_run_dir, test_name, success, stdout, stderr, duration)
    
    return success


def test_data_fetching(test_run_dir, quick_mode=True):
    """
    Test data fetching functionality
    
    Args:
        test_run_dir (str): Test run directory
        quick_mode (bool): Run in quick mode with small date range
        
    Returns:
        bool: Success flag
    """
    test_name = "data_fetching"
    logger.info(f"Running {test_name} test")
    
    # Use a recent small date range for quick testing
    if quick_mode:
        today = datetime.now()
        start_date = (today - timedelta(days=30)).strftime("%Y%m%d")
        end_date = today.strftime("%Y%m%d")
        exchanges = "SSE"  # Just one exchange for quick test
    else:
        # Use a larger range for comprehensive testing
        start_date = "20220101"  # Last 2 years
        end_date = datetime.now().strftime("%Y%m%d")
        exchanges = "SSE,SZSE"  # Multiple exchanges
    
    command = [
        "python", ULTIMATE_FETCHER,
        "--start-date", start_date,
        "--end-date", end_date,
        "--exchange", exchanges,
        "--verbose"
    ]
    
    success, stdout, stderr, duration = run_command(
        command, f"Data fetching test ({start_date} to {end_date})"
    )
    
    additional_data = {
        "start_date": start_date,
        "end_date": end_date,
        "exchanges": exchanges,
        "quick_mode": quick_mode
    }
    
    save_test_result(test_run_dir, test_name, success, stdout, stderr, duration, 
                   additional_data)
    
    return success


def test_performance_monitoring(test_run_dir):
    """
    Test performance monitoring functionality
    
    Args:
        test_run_dir (str): Test run directory
        
    Returns:
        bool: Success flag
    """
    test_name = "performance_monitoring"
    logger.info(f"Running {test_name} test")
    
    output_dir = os.path.join(test_run_dir, "performance_reports")
    os.makedirs(output_dir, exist_ok=True)
    
    command = [
        "python", MONITOR_PERFORMANCE,
        "--output-dir", output_dir,
        "--html-report",
        "--export-json"
    ]
    
    success, stdout, stderr, duration = run_command(
        command, "Performance monitoring test"
    )
    
    save_test_result(test_run_dir, test_name, success, stdout, stderr, duration)
    
    return success


def test_toolkit_commands(test_run_dir):
    """
    Test trade calendar toolkit commands
    
    Args:
        test_run_dir (str): Test run directory
        
    Returns:
        bool: Success flag
    """
    test_name = "toolkit_commands"
    logger.info(f"Running {test_name} test")
    
    # Test help command
    command = ["python", "trade_calendar_toolkit.py", "--help"]
    success, stdout, stderr, duration = run_command(
        command, "Toolkit help command"
    )
    
    save_test_result(test_run_dir, f"{test_name}_help", success, stdout, stderr, duration)
    
    # Test quicktest command
    command = ["python", "trade_calendar_toolkit.py", "quicktest", "--run-scenarios"]
    success, stdout, stderr, duration = run_command(
        command, "Toolkit quicktest command"
    )
    
    save_test_result(test_run_dir, f"{test_name}_quicktest", success, stdout, stderr, duration)
    
    return success


def create_test_summary(test_run_dir, test_results):
    """
    Create test summary
    
    Args:
        test_run_dir (str): Test run directory
        test_results (dict): Dictionary of test results
        
    Returns:
        str: Path to summary file
    """
    summary = {
        "timestamp": datetime.now().isoformat(),
        "overall_success": all(test_results.values()),
        "tests": test_results,
        "total_tests": len(test_results),
        "successful_tests": sum(1 for success in test_results.values() if success),
        "failed_tests": sum(1 for success in test_results.values() if not success)
    }
    
    # Calculate success rate
    if len(test_results) > 0:
        success_rate = (summary["successful_tests"] / summary["total_tests"]) * 100
        summary["success_rate"] = f"{success_rate:.2f}%"
    else:
        summary["success_rate"] = "N/A"
    
    # Save summary to JSON
    summary_file = os.path.join(test_run_dir, "test_summary.json")
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    
    # Create human-readable summary
    readable_summary = [
        "===== INTEGRATION TEST SUMMARY =====",
        f"Timestamp: {summary['timestamp']}",
        f"Overall Success: {'Yes' if summary['overall_success'] else 'No'}",
        f"Total Tests: {summary['total_tests']}",
        f"Successful Tests: {summary['successful_tests']}",
        f"Failed Tests: {summary['failed_tests']}",
        f"Success Rate: {summary['success_rate']}",
        "",
        "Test Results:"
    ]
    
    for test_name, success in test_results.items():
        status = "✓ PASSED" if success else "✗ FAILED"
        readable_summary.append(f"- {test_name}: {status}")
    
    # Save readable summary
    readable_file = os.path.join(test_run_dir, "test_summary.txt")
    with open(readable_file, "w") as f:
        f.write("\n".join(readable_summary))
    
    logger.info(f"Test summary saved to {summary_file} and {readable_file}")
    
    return readable_file


def print_summary(summary_file):
    """
    Print summary to console
    
    Args:
        summary_file (str): Path to summary file
    """
    try:
        with open(summary_file, "r") as f:
            print("\n" + f.read())
    except Exception as e:
        logger.error(f"Error reading summary file: {str(e)}")


def main():
    """
    Main function
    """
    parser = argparse.ArgumentParser(description="Trade Calendar System Integration Test")
    parser.add_argument('--quick', action='store_true', 
                      help="Run quick tests with minimal data")
    parser.add_argument('--skip-fetch', action='store_true',
                      help="Skip data fetching test (which can take longer)")
    
    args = parser.parse_args()
    
    try:
        logger.info("Starting system integration tests")
        
        # Ensure test directory exists
        test_run_dir = ensure_test_directory()
        logger.info(f"Test results will be saved to {test_run_dir}")
        
        # Run tests
        test_results = {}
        
        # Batch calculation test
        test_results["batch_calculation"] = test_batch_calculation(test_run_dir)
        
        # Data fetching test (optional)
        if not args.skip_fetch:
            test_results["data_fetching"] = test_data_fetching(test_run_dir, args.quick)
        
        # Performance monitoring test
        test_results["performance_monitoring"] = test_performance_monitoring(test_run_dir)
        
        # Toolkit commands test
        test_results["toolkit_commands"] = test_toolkit_commands(test_run_dir)
        
        # Create and print summary
        summary_file = create_test_summary(test_run_dir, test_results)
        print_summary(summary_file)
        
        logger.info("Integration tests completed")
        
        # Exit with appropriate code
        if all(test_results.values()):
            logger.info("All tests passed")
            return 0
        else:
            logger.error("One or more tests failed")
            return 1
        
    except Exception as e:
        logger.error(f"Error running integration tests: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
