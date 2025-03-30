# Stock Basic Fetcher Documentation

## Overview

The `stock_basic_fetcher.py` script is designed to retrieve stock basic information from Tushare API and store it in MongoDB. It's part of the quantdb project, which aims to provide a comprehensive data platform for quantitative finance research and analysis.

## Features

- Fetches stock basic information from Tushare API
- Filters stocks by market codes (e.g., SSE for Shanghai, SZSE for Shenzhen)
- Stores data in MongoDB with automatic schema mapping
- Provides verbose logging for debugging and monitoring
- Supports configuration through command line arguments

## Prerequisites

1. Python 3.8 or higher
2. MongoDB server
3. Tushare API token (configured in config.yaml)
4. Required Python packages: pymongo, tushare, loguru, PyYAML

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/snowspig/quantdb.git
   cd quantdb
   ```

2. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your Tushare API token and MongoDB connection in `config/config.yaml`

## Configuration

The script uses a YAML configuration file located at `config/config.yaml`. The configuration should include:

```yaml
mongodb:
  host: "your_mongodb_host"
  port: 27017
  username: "your_username"
  password: "your_password"
  auth_source: "admin"
  auth_mechanism: "SCRAM-SHA-1"

tushare:
  token: "your_tushare_token"
  api_url: "http://api.tushare.pro"
```

## Usage

Basic usage:

```bash
python stock_basic_fetcher.py
```

With options:

```bash
python stock_basic_fetcher.py \
  --verbose \
  --db-name tushare_data \
  --collection-name stock_basic \
  --market-codes SS,SZ
```

### Command Line Arguments

- `--verbose`: Enable detailed logging
- `--db-name`: Specify MongoDB database name (default: tushare_data)
- `--collection-name`: Specify MongoDB collection name (default: stock_basic)
- `--market-codes`: Filter stocks by market codes (comma-separated, e.g., SS,SZ)
- `--config`: Path to config file (default: config/config.yaml)
- `--interface-dir`: Path to interface definitions (default: config/interfaces)

## Example

```bash
# Fetch all stocks from Shanghai and Shenzhen exchanges with verbose logging
python stock_basic_fetcher.py --verbose --market-codes SS,SZ

# Store in a specific database and collection
python stock_basic_fetcher.py --db-name finance_data --collection-name stocks --market-codes SS
```

## Data Structure

The script fetches the following fields for each stock:

- `ts_code`: Stock code in Tushare format (e.g., 600000.SH)
- `symbol`: Stock code (e.g., 600000)
- `name`: Stock name in Chinese
- `comp_name`: Company name in Chinese
- `comp_name_en`: Company name in English
- `exchange`: Exchange code (SSE for Shanghai, SZSE for Shenzhen)
- `list_date`: Listing date (YYYYMMDD)
- `delist_date`: Delisting date (YYYYMMDD) or null
- `list_board`: Board code
- `list_board_name`: Board name (e.g., 主板, 创业板)
- `is_shsc`: Whether the stock is eligible for Shanghai-Hong Kong Stock Connect

Additional fields may be included based on the Tushare API response.

## MongoDB Collections

The data is stored in MongoDB with the following structure:

- Database: tushare_data (configurable via command line)
- Collection: stock_basic (configurable via command line)

## Troubleshooting

### Common Issues

1. **MongoDB Connection Error**:
   - Check if MongoDB is running
   - Verify connection string and credentials in config.yaml
   - Ensure network connectivity to MongoDB server

2. **Tushare API Error**:
   - Verify your Tushare token in config.yaml
   - Check if your Tushare account has access to the required data
   - Ensure network connectivity to Tushare API server

3. **No Data Returned**:
   - Check if market codes are valid (SS, SZ)
   - Verify that Tushare API is returning data for the requested parameters

## Source Code Explanation

The script is organized into several components:

1. **Configuration Management**: Loads settings from config.yaml
2. **Tushare Client**: Handles API requests to Tushare
3. **MongoDB Client**: Manages database connections and operations
4. **Main Function**: Orchestrates the data fetching and storage process

### Key Functions

- `fetch_stock_basic()`: Retrieves stock basic data from Tushare
- `save_to_mongodb()`: Stores the fetched data in MongoDB
- `filter_by_market_code()`: Filters stocks by specified market codes

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Contact

For questions or feedback, please open an issue on GitHub or contact the repository owner.
