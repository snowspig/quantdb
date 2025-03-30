# QuantDB

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive data platform for quantitative finance research and analysis based on Tushare API with MongoDB storage.

## Features

- **Multi-WAN Support**: Load balance API requests across multiple network interfaces
- **Tushare Integration**: Efficiently fetch financial data from Tushare API
- **MongoDB Storage**: Store and manage financial data in MongoDB
- **Market Filtering**: Filter stocks by market (SSE, SZSE) and other criteria
- **Comprehensive Logging**: Detailed logging for debugging and monitoring

## Project Structure

```
quantdb/
├── config/
│   ├── config.yaml             # Main configuration file
│   └── interfaces/             # Tushare interface definitions
├── data_fetcher/
│   ├── api_request.py         # API request handling with multi-WAN support
│   ├── data_updater.py        # Data update orchestration
│   └── tushare_client.py      # Tushare API client
├── storage/
│   ├── data_models.py         # Data models for financial data
│   └── mongodb_client.py      # MongoDB client
├── wan_manager/
│   ├── load_balancer.py       # Network load balancing
│   ├── port_allocator.py      # Port allocation for multiple WANs
│   └── wan_monitor.py         # WAN interface monitoring
├── docs/
│   └── stock_basic_fetcher_guide.md  # Documentation for stock basic fetcher
├── stock_basic_fetcher.py     # Script to fetch stock basic information
└── tushare_updater.py         # Comprehensive updater for all Tushare interfaces
```

## Getting Started

### Prerequisites

- Python 3.8+
- MongoDB 4.0+
- Tushare API token

### Installation

1. Clone the repository
   ```bash
   git clone https://github.com/snowspig/quantdb.git
   cd quantdb
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Configure your environment
   ```bash
   # Copy the sample config file and update with your credentials
   cp config/config.yaml.sample config/config.yaml
   # Edit config/config.yaml with your Tushare token and MongoDB credentials
   ```

### Usage

#### Fetching Stock Basic Information

```bash
python stock_basic_fetcher.py --verbose --market-codes SS,SZ
```

See [Stock Basic Fetcher Documentation](docs/stock_basic_fetcher_guide.md) for detailed usage.

## Documentation

Detailed documentation is available in the [docs](docs/) directory:

- [Stock Basic Fetcher Guide](docs/stock_basic_fetcher_guide.md)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- [Tushare](https://tushare.pro/) - Financial data API provider
- [MongoDB](https://www.mongodb.com/) - NoSQL database
