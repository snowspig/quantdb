# Trading Calendar Data Optimization Solution

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![MongoDB](https://img.shields.io/badge/MongoDB-4.4%2B-green.svg)](https://www.mongodb.com/)
[![TuShare](https://img.shields.io/badge/TuShare-API-orange.svg)](https://tushare.pro)

## Overview

This package provides an optimized solution for fetching trading calendar data from the TuShare API and storing it in MongoDB. The solution includes intelligent batch processing strategies, adaptive performance optimization, and comprehensive monitoring tools.

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd quantdb

# Install dependencies
pip install -r requirements.txt
```

### Configuration

Update your TuShare token and MongoDB connection in `config/config.yaml`:

```yaml
tushare:
  token: "your_tushare_token_here"

mongodb:
  uri: "mongodb://username:password@hostname:port/database"
  database: "quantdb"
  collection_prefix: "tushare_"
```

### Basic Usage

Use the unified toolkit interface:

```bash
# Fetch trading calendar data
python trade_calendar_toolkit.py fetch --start-date 20200101 --end-date 20231231 --exchange SSE,SZSE

# Monitor performance
python trade_calendar_toolkit.py monitor --html-report --export-json

# Run tests
python trade_calendar_toolkit.py test
```

## Main Components

### Core Modules

- **intelligent_batch_processing.py** - Smart batching strategies for efficient API usage
- **adaptive_batch.py** - Performance-based optimization algorithms

### Main Scripts

- **ultimate_trade_calendar_fetcher.py** - Primary optimized fetcher
- **monitor_trade_calendar_performance.py** - Visualization and performance analysis tool
- **trade_calendar_toolkit.py** - Unified command-line interface for all tools

### Testing Tools

- **quick_test_optimization.py** - Quick verification of batch strategies
- **test_optimization_solution.py** - Comprehensive test suite
- **integration_test.py** - System integration test

## Performance Features

- **Intelligent Batch Processing**: Automatically selects optimal strategy based on data size
- **Adaptive Optimization**: Adjusts batch sizes based on real-time performance
- **MongoDB Bulk Operations**: Efficiently handles large volumes of data
- **Comprehensive Error Handling**: Exponential backoff for API rate limits

## Batch Strategies

1. **single_batch**: For small datasets (< 1 year)
2. **year**: For medium datasets (1-5 years)
3. **exchange_year**: For large datasets (5+ years, multiple exchanges)

## Advanced Usage

### Custom Batch Strategy

```bash
python ultimate_trade_calendar_fetcher.py \
  --start-date 19900101 \
  --end-date 20240101 \
  --exchange SSE,SZSE,SHN,SZN \
  --batch-strategy exchange_year \
  --years-per-batch 5 \
  --mongo-chunk-size 1000
```

### Performance Monitoring

```bash
python monitor_trade_calendar_performance.py \
  --report-file trade_cal_performance_report.txt \
  --output-dir performance_reports \
  --html-report \
  --export-json
```

### Running Integration Tests

```bash
# Full integration test
python integration_test.py

# Quick test with smaller data sample
python integration_test.py --quick
```

## Documentation

- **[OPTIMIZATION_README.md](./OPTIMIZATION_README.md)** - Detailed optimization guide
- **[installation_guide.md](./installation_guide.md)** - Step-by-step installation instructions
- **[mongodb_connection_guide.md](./mongodb_connection_guide.md)** - MongoDB connection help

## Performance Results

Benchmarks from our testing show significant improvements:

- **API Call Throughput**: ~1100+ records/sec (3-4x improvement)
- **MongoDB Operations**: ~5-10x improvement with bulk operations
- **Overall Processing**: ~400+ records/sec (3x improvement)
- **Large Dataset Handling**: Successfully processes 35+ years of data across multiple exchanges

## Configuration Options

### Command Line Options

```
usage: ultimate_trade_calendar_fetcher.py [-h] [--start-date START_DATE]
                                        [--end-date END_DATE] [--exchange EXCHANGE]
                                        [--batch-strategy {year,exchange,exchange_year,single_batch}]
                                        [--years-per-batch YEARS_PER_BATCH]
                                        [--mongo-chunk-size MONGO_CHUNK_SIZE] [--verify-only]
                                        [--verbose]
```

### Config File Options

In `config/config.yaml`:

```yaml
batch_settings:
  years_per_batch: 5        # Process 5 years per batch by default
  max_batch_size: 5000      # Maximum records per batch
  max_retries: 3            # Maximum retry attempts
  retry_delay: 1.0          # Initial retry delay in seconds
  mongo_chunk_size: 1000    # MongoDB bulk operations chunk size
  batch_strategy: "year"    # Default batch strategy
```

## Troubleshooting

### MongoDB Connection Issues

```bash
# Test MongoDB connection
python test_mongo_auth.py

# Fix connection issues
python fix_mongo_connection.py
```

### TuShare API Issues

```bash
# Test TuShare token
python test_tushare_token.py

# Try direct HTTP approach
python direct_http_trade_cal.py
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
