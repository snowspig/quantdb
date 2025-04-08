# QuantDB

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A comprehensive data platform for quantitative finance research and analysis based on Tushare API with MongoDB storage.

## Features

- **Multi-WAN Support**: Load balance API requests across multiple network interfaces
- **Tushare Integration**: Efficiently fetch financial data from Tushare API
- **MongoDB Storage**: Store and manage financial data in MongoDB
- **Market Filtering**: Filter stocks by market (SSE, SZSE) and other criteria
- **Comprehensive Logging**: Detailed logging for debugging and monitoring
- **Connectivity Testing**: Built-in tools to test and verify multi-WAN functionality

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
├── core/
│   ├── mongodb_handler.py     # MongoDB connection handler
│   └── network_manager.py     # Network connectivity manager
├── docs/
│   ├── stock_basic_fetcher_guide.md  # Documentation for stock basic fetcher
│   └── multi_wan_testing.md   # Guide for Multi-WAN functionality testing
├── test_connections.py        # Test connections to APIs and databases
├── test_wan_url.py           # Test WAN functionality through URL access
├── test_wan_detection.py     # Detect and verify Multi-WAN configuration
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

# QuantDB 增强版

QuantDB是一个用于获取和管理股票数据的系统，现在支持多WAN口并行请求和自动选择最快WAN口功能。

## 主要特性

- **多WAN口支持**：可同时利用多个网络接口发送请求，提高请求效率
- **自动选择最快WAN口**：自动测试并记录最快的WAN口，优先使用最快接口
- **并行数据获取**：使用ThreadPoolExecutor实现高效的并行请求
- **智能失败重试**：当一个WAN口请求失败时，自动通过其他WAN口重试
- **技术指标计算**：自动计算常用技术指标如MACD、RSI、布林带等
- **灵活的运行模式**：支持最近、特定日期、日期范围、全量等多种获取模式

## 核心组件

1. **EnhancedFetcher**：基础增强型数据获取器，提供多WAN口支持
2. **TushareFetcher**：与Tushare API交互的专用数据获取器
3. **DailyFetcher**：专门用于获取日线数据的实现

## 主要命令

```bash
# 测试所有WAN口，并行模式
python main.py --test-wan --wan-mode=parallel

# 测试所有WAN口，串行模式
python main.py --test-wan --wan-mode=serial

# 并行和串行对比测试
python main.py --test-wan --wan-mode=both

# 寻找最快的WAN口
python main.py --find-fastest-wan --test-count=3

# 通过最快WAN口发送API请求
python main.py --api-request --api-name=trade_cal
```

## 使用示例

### 基础使用 - TushareFetcher

```python
from core.tushare_fetcher import TushareFetcher

# 创建Tushare数据获取器
fetcher = TushareFetcher("stock_basic")

# 使用最快WAN口获取数据
df_fastest = fetcher.fetch_data(list_status="L", exchange="SSE")

# 使用指定WAN口获取数据
df_wan0 = fetcher.fetch_data_via_wan(0, list_status="L")

# 批量获取多个股票的数据
tasks = [
    {"ts_code": "000001.SZ", "start_date": "20230101", "end_date": "20230201"},
    {"ts_code": "000002.SZ", "start_date": "20230101", "end_date": "20230201"}
]
saved_count = fetcher.parallel_fetch(tasks)
```

### 日线数据获取 - DailyFetcher

```python
from core.daily_fetcher import DailyFetcher

# 创建日线数据获取器
daily_fetcher = DailyFetcher()

# 获取单只股票的日线数据
df = daily_fetcher.fetch_data(
    ts_code="000001.SZ", 
    start_date="20230101", 
    end_date="20230131"
)

# 获取指定日期的全市场数据
df_date = daily_fetcher.fetch_data(trade_date="20230103")

# 批量获取多只股票的数据
daily_fetcher.batch_fetch_stocks(
    ["000001.SZ", "000002.SZ"], 
    start_date="20230101", 
    end_date="20230131"
)

# 获取日期范围内的数据
daily_fetcher.run(
    mode="range", 
    start_date="20230101", 
    end_date="20230131"
)

# 获取最新交易日数据
result = daily_fetcher.run(mode="recent")
```

## 技术指标

DailyFetcher 自动计算以下技术指标：

- **移动平均线**：MA5, MA10, MA20, MA30, MA60
- **成交量均线**：Vol_MA5, Vol_MA10
- **MACD指标**：MACD_DIF, MACD_DEA, MACD_BAR
- **RSI指标**：RSI_6, RSI_12, RSI_24
- **布林带**：BOLL_MID, BOLL_UPPER, BOLL_LOWER, BOLL_WIDTH, BOLL_PB

## 配置

系统配置文件位于 `config/config.yaml`，包含以下关键配置：

```yaml
tushare:
  token: "your_tushare_token"
  api_url: "http://api.waditu.com"
  rate_limit: 0.6

wan:
  enabled: true
  use_fastest_wan: true
  retry_other_wan: true

fetcher:
  thread_count: 5
  batch_size: 100
  timeout: 30
  retry_count: 3
```

## 开发

### 创建新的数据获取器

```python
from core.tushare_fetcher import TushareFetcher

class MyCustomFetcher(TushareFetcher):
    def __init__(self, api_name="my_api", config_path=None, silent=False):
        super().__init__(api_name, config_path, silent)
        
    def process_data(self, data):
        # 自定义数据处理逻辑
        data = super().process_data(data)
        # 添加更多处理...
        return data
```

## 依赖

- Python 3.6+
- pandas
- pymongo
- tushare/xcsc_tushare
- requests

## 贡献

欢迎提交Pull Request或Issue来改进本项目。
