# Installation Guide for Trade Calendar Fetcher

## Prerequisites

1. **Python 3.8+**
2. **MongoDB** - Either locally installed or a remote connection
3. **TuShare API Token** - Register at https://tushare.pro/register to get a token

## Installation Steps

### 1. Set up a Virtual Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows
venv\Scripts\activate
# On Unix or MacOS
source venv/bin/activate
```

### 2. Install Required Packages

```bash
# Install from requirements.txt
pip install -r requirements.txt

# Or install packages directly
pip install pandas pymongo pyyaml matplotlib numpy tushare requests tabulate python-dotenv
```

### 3. Configure Settings

1. Create a `.env` file in the project root with the following content:

```
TUSHARE_TOKEN=your_tushare_token_here
MONGO_URI=mongodb://username:password@hostname:port/database
```

2. Or update the `config/config.yaml` file directly with your credentials:

```yaml
tushare:
  token: "your_tushare_token_here"

mongodb:
  uri: "mongodb://username:password@hostname:port/database"
  database: "quantdb"
  collection_prefix: "tushare_"

batch_settings:
  years_per_batch: 5
  max_batch_size: 5000
  max_retries: 3
  retry_delay: 1.0
  mongo_chunk_size: 1000
  batch_strategy: "exchange_year"
```

## Verify Installation

### 1. Test MongoDB Connection

```bash
python test_mongo_auth.py
```

### 2. Test TuShare API Connection

```bash
python test_tushare_token.py
```

### 3. Verify Optimization System

```bash
python quick_test_optimization.py --run-scenarios
```

## Usage Examples

### Basic Usage

Fetch trading calendar data for the Shanghai Stock Exchange (SSE) for 2023:

```bash
python ultimate_trade_calendar_fetcher.py --start-date 20230101 --end-date 20231231 --exchange SSE
```

### Advanced Usage

Fetch data for multiple exchanges with specific optimization settings:

```bash
python ultimate_trade_calendar_fetcher.py \
  --start-date 19900101 \
  --end-date 20240101 \
  --exchange SSE,SZSE \
  --batch-strategy exchange_year \
  --years-per-batch 5 \
  --mongo-chunk-size 1000 \
  --verbose
```

### Performance Monitoring

Generate performance reports and visualizations:

```bash
python monitor_trade_calendar_performance.py --html-report --export-json
```

## Troubleshooting

### MongoDB Connection Issues

If you encounter MongoDB connection problems:

1. Verify your connection string in .env or config.yaml
2. Check that your MongoDB server is running
3. Run `python fix_mongo_connection.py` to diagnose connection issues

### TuShare API Issues

If you have problems with the TuShare API:

1. Verify your token is valid with `python test_tushare_token.py`
2. Check that you have sufficient API credits at https://tushare.pro/user/usage
3. Try the direct HTTP approach with `python direct_http_trade_cal.py`

### Performance Issues

If you experience slow performance:

1. Try different batch strategies with `--batch-strategy`
2. Adjust years per batch with `--years-per-batch`
3. Modify MongoDB chunk size with `--mongo-chunk-size`
4. Run `python test_optimization_solution.py` to find optimal settings

## Updating the System

To update to the latest version:

```bash
git pull origin main
pip install -r requirements.txt --upgrade
```

## Additional Resources

- [OPTIMIZATION_README.md](./OPTIMIZATION_README.md) - Detailed optimization guide
- [README_OPTIMIZATION.md](./README_OPTIMIZATION.md) - Technical details of performance improvements
- [mongodb_connection_guide.md](./mongodb_connection_guide.md) - Comprehensive MongoDB connection guide
