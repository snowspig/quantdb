#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trade Calendar Toolkit

A unified command-line interface for all trade calendar management tools.
This script provides access to fetching, optimization, monitoring, and testing tools.
"""

import os
import sys
import argparse
import logging
import subprocess
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script paths
ULTIMATE_FETCHER = "ultimate_trade_calendar_fetcher.py"
MONITOR_PERFORMANCE = "monitor_trade_calendar_performance.py"
QUICK_TEST = "quick_test_optimization.py"
TEST_OPTIMIZATION = "test_optimization_solution.py"


def run_command(command):
    """
    Run a command and return the result
    
    Args:
        command (list): Command and arguments to run
        
    Returns:
        tuple: (exit_code, stdout, stderr)
    """
    try:
        logger.info(f"Running: {' '.join(command)}")
        result = subprocess.run(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        logger.error(f"Error running command: {str(e)}")
        return 1, "", str(e)


def print_command_output(exit_code, stdout, stderr):
    """
    Print command output
    
    Args:
        exit_code (int): Command exit code
        stdout (str): Command standard output
        stderr (str): Command standard error
    """
    if stdout:
        print("\nOutput:\n")
        print(stdout)
    
    if stderr:
        print("\nErrors:\n")
        print(stderr)
    
    print(f"\nCommand {'succeeded' if exit_code == 0 else 'failed'} with exit code {exit_code}")


def fetch_data(args):
    """
    Fetch trading calendar data
    
    Args:
        args (Namespace): Command line arguments
    """
    command = ["python", ULTIMATE_FETCHER]
    
    # Add arguments
    if args.start_date:
        command.extend(["--start-date", args.start_date])
    
    if args.end_date:
        command.extend(["--end-date", args.end_date])
    
    if args.exchange:
        command.extend(["--exchange", args.exchange])
    
    if args.batch_strategy:
        command.extend(["--batch-strategy", args.batch_strategy])
    
    if args.years_per_batch:
        command.extend(["--years-per-batch", str(args.years_per_batch)])
    
    if args.mongo_chunk_size:
        command.extend(["--mongo-chunk-size", str(args.mongo_chunk_size)])
    
    if args.verbose:
        command.append("--verbose")
    
    # Run command
    exit_code, stdout, stderr = run_command(command)
    print_command_output(exit_code, stdout, stderr)


def monitor_performance(args):
    """
    Monitor trade calendar performance
    
    Args:
        args (Namespace): Command line arguments
    """
    command = ["python", MONITOR_PERFORMANCE]
    
    # Add arguments
    if args.report_file:
        command.extend(["--report-file", args.report_file])
    
    if args.output_dir:
        command.extend(["--output-dir", args.output_dir])
    
    if args.html_report:
        command.append("--html-report")
    
    if args.export_json:
        command.append("--export-json")
    
    # Run command
    exit_code, stdout, stderr = run_command(command)
    print_command_output(exit_code, stdout, stderr)


def test_optimization(args):
    """
    Test optimization strategies
    
    Args:
        args (Namespace): Command line arguments
    """
    command = ["python", TEST_OPTIMIZATION]
    
    # Add arguments
    if args.quick_test:
        command.append("--quick-test")
    
    if args.verify_only:
        command.append("--verify-only")
    
    # Run command
    exit_code, stdout, stderr = run_command(command)
    print_command_output(exit_code, stdout, stderr)


def quick_test(args):
    """
    Quick test of batch calculation
    
    Args:
        args (Namespace): Command line arguments
    """
    command = ["python", QUICK_TEST]
    
    # Add arguments
    if args.start_date:
        command.extend(["--start-date", args.start_date])
    
    if args.end_date:
        command.extend(["--end-date", args.end_date])
    
    if args.exchange:
        command.extend(["--exchange", args.exchange])
    
    if args.strategy:
        command.extend(["--strategy", args.strategy])
    
    if args.years_per_batch:
        command.extend(["--years-per-batch", str(args.years_per_batch)])
    
    if args.run_scenarios:
        command.append("--run-scenarios")
    
    if args.output:
        command.extend(["--output", args.output])
    
    # Run command
    exit_code, stdout, stderr = run_command(command)
    print_command_output(exit_code, stdout, stderr)


def setup_fetch_parser(subparsers):
    """
    Set up fetch subcommand parser
    
    Args:
        subparsers: Subparsers object
    """
    fetch_parser = subparsers.add_parser('fetch', help='Fetch trading calendar data')
    fetch_parser.add_argument('--start-date', type=str, help='Start date (YYYYMMDD)')
    fetch_parser.add_argument('--end-date', type=str, help='End date (YYYYMMDD)')
    fetch_parser.add_argument('--exchange', type=str, help='Exchange codes (comma-separated)')
    fetch_parser.add_argument('--batch-strategy', type=str, 
                           choices=['year', 'exchange', 'exchange_year', 'single_batch'],
                           help='Batch strategy')
    fetch_parser.add_argument('--years-per-batch', type=int, help='Years per batch')
    fetch_parser.add_argument('--mongo-chunk-size', type=int, help='MongoDB chunk size')
    fetch_parser.add_argument('--verbose', action='store_true', help='Verbose output')
    fetch_parser.set_defaults(func=fetch_data)


def setup_monitor_parser(subparsers):
    """
    Set up monitor subcommand parser
    
    Args:
        subparsers: Subparsers object
    """
    monitor_parser = subparsers.add_parser('monitor', help='Monitor performance')
    monitor_parser.add_argument('--report-file', type=str, 
                                default="trade_cal_performance_report.txt",
                                help='Path to performance report file')
    monitor_parser.add_argument('--output-dir', type=str, 
                                default="performance_reports",
                                help='Directory to save visualizations and reports')
    monitor_parser.add_argument('--html-report', action='store_true',
                                help='Generate HTML report')
    monitor_parser.add_argument('--export-json', action='store_true',
                                help='Export performance data as JSON')
    monitor_parser.set_defaults(func=monitor_performance)


def setup_test_parser(subparsers):
    """
    Set up test subcommand parser
    
    Args:
        subparsers: Subparsers object
    """
    test_parser = subparsers.add_parser('test', help='Test optimization strategies')
    test_parser.add_argument('--quick-test', action='store_true',
                              help='Run a quick test only')
    test_parser.add_argument('--verify-only', action='store_true',
                              help='Only verify the optimization')
    test_parser.set_defaults(func=test_optimization)


def setup_quicktest_parser(subparsers):
    """
    Set up quicktest subcommand parser
    
    Args:
        subparsers: Subparsers object
    """
    quicktest_parser = subparsers.add_parser('quicktest', 
                                          help='Quick test of batch calculation')
    quicktest_parser.add_argument('--start-date', type=str, 
                                help='Start date (YYYYMMDD)')
    quicktest_parser.add_argument('--end-date', type=str, 
                                help='End date (YYYYMMDD)')
    quicktest_parser.add_argument('--exchange', type=str, 
                                help='Exchange codes (comma-separated)')
    quicktest_parser.add_argument('--strategy', type=str, 
                                choices=['year', 'exchange', 'exchange_year', 'single_batch'],
                                help='Batch strategy')
    quicktest_parser.add_argument('--years-per-batch', type=int, 
                                help='Years per batch')
    quicktest_parser.add_argument('--run-scenarios', action='store_true',
                                help='Run test scenarios')
    quicktest_parser.add_argument('--output', type=str, 
                                help='Output file for batch configuration')
    quicktest_parser.set_defaults(func=quick_test)


def main():
    """
    Main function
    """
    parser = argparse.ArgumentParser(
        description="Trade Calendar Toolkit - Unified interface for trade calendar management"
    )
    
    parser.add_argument('-v', '--version', action='version', version='Trade Calendar Toolkit v1.0.0')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Set up command parsers
    setup_fetch_parser(subparsers)
    setup_monitor_parser(subparsers)
    setup_test_parser(subparsers)
    setup_quicktest_parser(subparsers)
    
    # Parse arguments
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return
    
    # Run function
    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation canceled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
