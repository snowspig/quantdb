#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trade Calendar Performance Monitoring Script

This script monitors and visualizes the performance of the trade calendar fetching process.
It reads performance data from the trade_cal_performance_report.txt file and generates
real-time performance charts and insights.
"""

import os
import re
import time
import json
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional

try:
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    from tabulate import tabulate
    VISUALIZATION_ENABLED = True
except ImportError:
    print("Visualization libraries not available. Installing required packages...")
    import subprocess
    subprocess.run(["pip", "install", "matplotlib", "numpy", "pandas", "tabulate"])
    try:
        import matplotlib.pyplot as plt
        import numpy as np
        import pandas as pd
        from tabulate import tabulate
        VISUALIZATION_ENABLED = True
    except ImportError:
        print("Failed to install visualization libraries. Running in text-only mode.")
        VISUALIZATION_ENABLED = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_performance_report(report_path: str) -> Dict:
    """
    Parse the performance report file into a structured dictionary
    
    Args:
        report_path (str): Path to the performance report file
        
    Returns:
        dict: Structured performance data
    """
    if not os.path.exists(report_path):
        logger.error(f"Performance report file not found: {report_path}")
        return {}
    
    try:
        with open(report_path, 'r') as f:
            content = f.read()
        
        # Extract sections
        sections = {
            'date_range': re.search(r'Date Range:\s+(.+)\s+to\s+(.+)', content),
            'overall': {
                'duration': re.search(r'Total execution time:\s+([\d\.]+)s', content),
                'records': re.search(r'Total records processed:\s+([\d,]+)', content),
                'throughput': re.search(r'Overall throughput:\s+([\d\.]+)\s+records/sec', content)
            },
            'api_calls': {
                'count': re.search(r'Total API calls:\s+(\d+)', content),
                'records': re.search(r'Total records retrieved:\s+([\d,]+)', content),
                'avg_records': re.search(r'Avg records per call:\s+([\d\.]+)', content),
                'avg_duration': re.search(r'Avg duration per call:\s+([\d\.]+)s', content),
                'avg_throughput': re.search(r'Avg throughput:\s+([\d\.]+)\s+records/sec', content),
                'best_throughput': re.search(r'Best throughput:\s+([\d\.]+)\s+records/sec', content),
                'worst_throughput': re.search(r'Worst throughput:\s+([\d\.]+)\s+records/sec', content)
            },
            'batches': {
                'count': re.search(r'Total batches:\s+(\d+)', content),
                'records': re.search(r'Total records processed:\s+([\d,]+)', content),
                'avg_records': re.search(r'Avg records per batch:\s+([\d\.]+)', content),
                'avg_years': re.search(r'Avg years per batch:\s+([\d\.]+)', content),
                'avg_duration': re.search(r'Avg duration per batch:\s+([\d\.]+)s', content),
                'avg_throughput': re.search(r'Avg batch throughput:\s+([\d\.]+)\s+records/sec', content),
                'best_throughput': re.search(r'Best batch throughput:\s+([\d\.]+)\s+records/sec', content)
            },
            'mongodb': {
                'count': re.search(r'Total operations:\s+(\d+)', content),
                'records': re.search(r'Total records processed:\s+([\d,]+)', content),
                'avg_records': re.search(r'Avg records per operation:\s+([\d\.]+)', content),
                'avg_duration': re.search(r'Avg duration per operation:\s+([\d\.]+)s', content),
                'avg_throughput': re.search(r'Avg MongoDB throughput:\s+([\d\.]+)\s+records/sec', content)
            },
            'recommendations': {
                'years_per_batch': re.search(r'Best years per batch:\s+(\d+)\s+years', content),
                'strategy': re.search(r'Recommended strategy:\s+(.+)', content),
                'chunk_size': re.search(r'Optimal MongoDB chunk size:\s+(\d+)', content)
            }
        }
        
        # Parse performance by exchange-year
        exchange_year_pattern = r'- ([A-Z]+):([\d-]+):\s+(\d+)\s+records\s+in\s+([\d\.]+)s\s+\(([\d\.]+)\s+records/sec\)'
        exchange_year_matches = re.findall(exchange_year_pattern, content)
        exchange_year_data = []
        
        for match in exchange_year_matches:
            exchange_year_data.append({
                'exchange': match[0],
                'year_range': match[1],
                'records': int(match[2]),
                'duration': float(match[3]),
                'throughput': float(match[4])
            })
        
        # Build result structure
        result = {
            'date_range': {
                'start_date': sections['date_range'].group(1) if sections['date_range'] else None,
                'end_date': sections['date_range'].group(2) if sections['date_range'] else None
            },
            'overall': {
                'duration': float(sections['overall']['duration'].group(1)) if sections['overall']['duration'] else 0,
                'records': int(sections['overall']['records'].group(1).replace(',', '')) if sections['overall']['records'] else 0,
                'throughput': float(sections['overall']['throughput'].group(1)) if sections['overall']['throughput'] else 0
            },
            'api_calls': {
                'count': int(sections['api_calls']['count'].group(1)) if sections['api_calls']['count'] else 0,
                'records': int(sections['api_calls']['records'].group(1).replace(',', '')) if sections['api_calls']['records'] else 0,
                'avg_records': float(sections['api_calls']['avg_records'].group(1)) if sections['api_calls']['avg_records'] else 0,
                'avg_duration': float(sections['api_calls']['avg_duration'].group(1)) if sections['api_calls']['avg_duration'] else 0,
                'avg_throughput': float(sections['api_calls']['avg_throughput'].group(1)) if sections['api_calls']['avg_throughput'] else 0,
                'best_throughput': float(sections['api_calls']['best_throughput'].group(1)) if sections['api_calls']['best_throughput'] else 0,
                'worst_throughput': float(sections['api_calls']['worst_throughput'].group(1)) if sections['api_calls']['worst_throughput'] else 0
            },
            'batches': {
                'count': int(sections['batches']['count'].group(1)) if sections['batches']['count'] else 0,
                'records': int(sections['batches']['records'].group(1).replace(',', '')) if sections['batches']['records'] else 0,
                'avg_records': float(sections['batches']['avg_records'].group(1)) if sections['batches']['avg_records'] else 0,
                'avg_years': float(sections['batches']['avg_years'].group(1)) if sections['batches']['avg_years'] else 0,
                'avg_duration': float(sections['batches']['avg_duration'].group(1)) if sections['batches']['avg_duration'] else 0,
                'avg_throughput': float(sections['batches']['avg_throughput'].group(1)) if sections['batches']['avg_throughput'] else 0,
                'best_throughput': float(sections['batches']['best_throughput'].group(1)) if sections['batches']['best_throughput'] else 0
            },
            'mongodb': {
                'count': int(sections['mongodb']['count'].group(1)) if sections['mongodb']['count'] else 0,
                'records': int(sections['mongodb']['records'].group(1).replace(',', '')) if sections['mongodb']['records'] else 0,
                'avg_records': float(sections['mongodb']['avg_records'].group(1)) if sections['mongodb']['avg_records'] else 0,
                'avg_duration': float(sections['mongodb']['avg_duration'].group(1)) if sections['mongodb']['avg_duration'] else 0,
                'avg_throughput': float(sections['mongodb']['avg_throughput'].group(1)) if sections['mongodb']['avg_throughput'] else 0
            },
            'recommendations': {
                'years_per_batch': int(sections['recommendations']['years_per_batch'].group(1)) if sections['recommendations']['years_per_batch'] else 0,
                'strategy': sections['recommendations']['strategy'].group(1) if sections['recommendations']['strategy'] else '',
                'chunk_size': int(sections['recommendations']['chunk_size'].group(1)) if sections['recommendations']['chunk_size'] else 0
            },
            'exchange_year_data': exchange_year_data
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error parsing performance report: {str(e)}")
        return {}


def generate_performance_summary(data: Dict) -> str:
    """
    Generate a text-based performance summary
    
    Args:
        data (dict): Parsed performance data
        
    Returns:
        str: Formatted performance summary
    """
    if not data:
        return "No performance data available"
    
    summary = [
        "===== PERFORMANCE SUMMARY =====",
        f"Date Range: {data['date_range']['start_date']} to {data['date_range']['end_date']}",
        "",
        "Overall Performance:",
        f"- Duration: {data['overall']['duration']:.2f}s ({data['overall']['duration']/60:.2f} minutes)",
        f"- Records: {data['overall']['records']:,}",
        f"- Throughput: {data['overall']['throughput']:.2f} records/sec",
        "",
        "Throughput Comparison:",
        f"- API Throughput: {data['api_calls']['avg_throughput']:.2f} records/sec",
        f"- Batch Throughput: {data['batches']['avg_throughput']:.2f} records/sec",
        f"- MongoDB Throughput: {data['mongodb']['avg_throughput']:.2f} records/sec",
        "",
        "Key Recommendations:",
        f"- Best Years Per Batch: {data['recommendations']['years_per_batch']} years",
        f"- Recommended Strategy: {data['recommendations']['strategy']}",
        f"- Optimal MongoDB Chunk Size: {data['recommendations']['chunk_size']}"
    ]
    
    return "\n".join(summary)


def plot_performance_metrics(data: Dict, output_dir: str = "."):
    """
    Generate performance visualizations
    
    Args:
        data (dict): Parsed performance data
        output_dir (str): Directory to save plots
    """
    if not VISUALIZATION_ENABLED:
        logger.warning("Visualization libraries not available. Skipping plot generation.")
        return
    
    if not data:
        logger.warning("No performance data available for visualization")
        return
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # 1. Throughput Comparison Bar Chart
        plt.figure(figsize=(12, 6))
        throughputs = [
            data['api_calls']['avg_throughput'],
            data['batches']['avg_throughput'],
            data['mongodb']['avg_throughput'],
            data['overall']['throughput']
        ]
        labels = ['API Calls', 'Batches', 'MongoDB', 'Overall']
        colors = ['#4285F4', '#0F9D58', '#F4B400', '#DB4437']
        
        plt.bar(labels, throughputs, color=colors)
        plt.title('Throughput Comparison')
        plt.ylabel('Records/sec')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        
        # Add values on top of bars
        for i, v in enumerate(throughputs):
            plt.text(i, v + 20, f"{v:.2f}", ha='center')
        
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, 'throughput_comparison.png'))
        plt.close()
        
        # 2. Exchange-Year Performance
        if data['exchange_year_data']:
            # Convert to DataFrame for easier manipulation
            df = pd.DataFrame(data['exchange_year_data'])
            
            # Sort by throughput (descending)
            df = df.sort_values('throughput', ascending=False)
            
            plt.figure(figsize=(14, 8))
            
            # Plot by exchange and year range
            exchanges = sorted(df['exchange'].unique())
            colors = plt.cm.tab10(np.linspace(0, 1, len(exchanges)))
            color_map = {ex: color for ex, color in zip(exchanges, colors)}
            
            bars = plt.bar(df.index, df['throughput'], color=[color_map[ex] for ex in df['exchange']])
            
            plt.title('Performance by Exchange and Year Range')
            plt.xlabel('Exchange-Year')
            plt.ylabel('Throughput (records/sec)')
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            
            # Add labels
            plt.xticks(df.index, [f"{row['exchange']}\n{row['year_range']}" for _, row in df.iterrows()], rotation=45)
            
            # Add legend
            legend_handles = [plt.Rectangle((0,0), 1, 1, color=color_map[ex]) for ex in exchanges]
            plt.legend(legend_handles, exchanges, loc='upper right')
            
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'exchange_year_performance.png'))
            plt.close()
            
            # 3. Performance by Exchange (aggregated)
            exchange_perf = df.groupby('exchange')['throughput'].mean().reset_index()
            exchange_perf = exchange_perf.sort_values('throughput', ascending=False)
            
            plt.figure(figsize=(10, 6))
            plt.bar(exchange_perf['exchange'], exchange_perf['throughput'], 
                   color=plt.cm.tab10(np.linspace(0, 1, len(exchange_perf))))
            plt.title('Average Throughput by Exchange')
            plt.xlabel('Exchange')
            plt.ylabel('Avg Throughput (records/sec)')
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            
            # Add values on top of bars
            for i, v in enumerate(exchange_perf['throughput']):
                plt.text(i, v + 10, f"{v:.2f}", ha='center')
            
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, 'exchange_performance.png'))
            plt.close()
            
        logger.info(f"Performance visualizations saved to {output_dir}")
            
    except Exception as e:
        logger.error(f"Error generating visualizations: {str(e)}")


def export_performance_data(data: Dict, output_file: str):
    """
    Export performance data to JSON file
    
    Args:
        data (dict): Performance data
        output_file (str): Output file path
    """
    try:
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Performance data exported to {output_file}")
    except Exception as e:
        logger.error(f"Error exporting performance data: {str(e)}")


def generate_html_report(data: Dict, output_file: str):
    """
    Generate an HTML report from performance data
    
    Args:
        data (dict): Performance data
        output_file (str): Output HTML file path
    """
    if not data:
        logger.warning("No performance data available for HTML report")
        return
    
    try:
        # Basic HTML template
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Trade Calendar Performance Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                h1 {{ color: #2c3e50; }}
                h2 {{ color: #3498db; margin-top: 30px; }}
                table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .metric {{ font-weight: bold; }}
                .value {{ font-family: monospace; }}
                .summary {{ background-color: #eaf2f8; padding: 15px; border-radius: 5px; }}
                .recommendation {{ background-color: #e8f8f5; padding: 10px; margin-top: 5px; border-left: 4px solid #1abc9c; }}
            </style>
        </head>
        <body>
            <h1>Trade Calendar Performance Report</h1>
            <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="summary">
                <h2>Performance Summary</h2>
                <p><span class="metric">Date Range:</span> <span class="value">{data['date_range']['start_date']} to {data['date_range']['end_date']}</span></p>
                <p><span class="metric">Total Duration:</span> <span class="value">{data['overall']['duration']:.2f} seconds ({data['overall']['duration']/60:.2f} minutes)</span></p>
                <p><span class="metric">Total Records:</span> <span class="value">{data['overall']['records']:,}</span></p>
                <p><span class="metric">Overall Throughput:</span> <span class="value">{data['overall']['throughput']:.2f} records/sec</span></p>
            </div>
            
            <h2>API Call Performance</h2>
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                <tr><td>Total API Calls</td><td>{data['api_calls']['count']}</td></tr>
                <tr><td>Total Records Retrieved</td><td>{data['api_calls']['records']:,}</td></tr>
                <tr><td>Average Records per Call</td><td>{data['api_calls']['avg_records']:.2f}</td></tr>
                <tr><td>Average Duration per Call</td><td>{data['api_calls']['avg_duration']:.2f} seconds</td></tr>
                <tr><td>Average Throughput</td><td>{data['api_calls']['avg_throughput']:.2f} records/sec</td></tr>
                <tr><td>Best Throughput</td><td>{data['api_calls']['best_throughput']:.2f} records/sec</td></tr>
                <tr><td>Worst Throughput</td><td>{data['api_calls']['worst_throughput']:.2f} records/sec</td></tr>
            </table>
            
            <h2>Batch Processing Performance</h2>
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                <tr><td>Total Batches</td><td>{data['batches']['count']}</td></tr>
                <tr><td>Total Records Processed</td><td>{data['batches']['records']:,}</td></tr>
                <tr><td>Average Records per Batch</td><td>{data['batches']['avg_records']:.2f}</td></tr>
                <tr><td>Average Years per Batch</td><td>{data['batches']['avg_years']:.2f}</td></tr>
                <tr><td>Average Duration per Batch</td><td>{data['batches']['avg_duration']:.2f} seconds</td></tr>
                <tr><td>Average Batch Throughput</td><td>{data['batches']['avg_throughput']:.2f} records/sec</td></tr>
                <tr><td>Best Batch Throughput</td><td>{data['batches']['best_throughput']:.2f} records/sec</td></tr>
            </table>
            
            <h2>MongoDB Performance</h2>
            <table>
                <tr><th>Metric</th><th>Value</th></tr>
                <tr><td>Total Operations</td><td>{data['mongodb']['count']}</td></tr>
                <tr><td>Total Records Processed</td><td>{data['mongodb']['records']:,}</td></tr>
                <tr><td>Average Records per Operation</td><td>{data['mongodb']['avg_records']:.2f}</td></tr>
                <tr><td>Average Duration per Operation</td><td>{data['mongodb']['avg_duration']:.2f} seconds</td></tr>
                <tr><td>Average MongoDB Throughput</td><td>{data['mongodb']['avg_throughput']:.2f} records/sec</td></tr>
            </table>
        """
        
        # Add Exchange-Year Performance Table
        if data['exchange_year_data']:
            exchange_year_table = """
            <h2>Performance by Exchange and Year Range</h2>
            <table>
                <tr>
                    <th>Exchange</th>
                    <th>Year Range</th>
                    <th>Records</th>
                    <th>Duration (s)</th>
                    <th>Throughput (records/sec)</th>
                </tr>
            """
            
            # Sort by throughput (descending)
            sorted_data = sorted(data['exchange_year_data'], key=lambda x: x['throughput'], reverse=True)
            
            for item in sorted_data:
                exchange_year_table += f"""
                <tr>
                    <td>{item['exchange']}</td>
                    <td>{item['year_range']}</td>
                    <td>{item['records']:,}</td>
                    <td>{item['duration']:.2f}</td>
                    <td>{item['throughput']:.2f}</td>
                </tr>
                """
            
            exchange_year_table += "</table>"
            html += exchange_year_table
        
        # Add Recommendations
        html += f"""
            <h2>Optimization Recommendations</h2>
            <div class="recommendation">
                <p><span class="metric">Best Years per Batch:</span> <span class="value">{data['recommendations']['years_per_batch']} years</span></p>
                <p><span class="metric">Recommended Strategy:</span> <span class="value">{data['recommendations']['strategy']}</span></p>
                <p><span class="metric">Optimal MongoDB Chunk Size:</span> <span class="value">{data['recommendations']['chunk_size']}</span></p>
            </div>
            
            <p style="margin-top: 30px; text-align: center; color: #7f8c8d;">Generated by Monitor Trade Calendar Performance script</p>
        </body>
        </html>
        """
        
        with open(output_file, 'w') as f:
            f.write(html)
        
        logger.info(f"HTML report saved to {output_file}")
        
    except Exception as e:
        logger.error(f"Error generating HTML report: {str(e)}")


def main():
    """
    Main function
    """
    parser = argparse.ArgumentParser(description="Monitor and visualize trade calendar performance")
    parser.add_argument(
        '--report-file', 
        type=str, 
        default="trade_cal_performance_report.txt",
        help="Path to performance report file"
    )
    parser.add_argument(
        '--output-dir', 
        type=str, 
        default="performance_reports",
        help="Directory to save visualizations and reports"
    )
    parser.add_argument(
        '--html-report', 
        action='store_true',
        help="Generate HTML report"
    )
    parser.add_argument(
        '--export-json', 
        action='store_true',
        help="Export performance data as JSON"
    )
    
    args = parser.parse_args()
    
    try:
        # Create output directory
        os.makedirs(args.output_dir, exist_ok=True)
        
        # Parse performance report
        logger.info(f"Parsing performance report: {args.report_file}")
        performance_data = parse_performance_report(args.report_file)
        
        if not performance_data:
            logger.error("No performance data found")
            return
        
        # Generate summary
        summary = generate_performance_summary(performance_data)
        print(summary)
        
        # Generate visualizations
        logger.info("Generating performance visualizations...")
        plot_performance_metrics(performance_data, args.output_dir)
        
        # Export as JSON if requested
        if args.export_json:
            json_path = os.path.join(args.output_dir, "performance_data.json")
            export_performance_data(performance_data, json_path)
        
        # Generate HTML report if requested
        if args.html_report:
            html_path = os.path.join(args.output_dir, "performance_report.html")
            generate_html_report(performance_data, html_path)
        
        logger.info("Performance monitoring complete")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return


if __name__ == "__main__":
    main()
