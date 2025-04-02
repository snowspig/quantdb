#!/usr/bin/env python
"""
日线行情数据获取器 - 获取日线行情数据并保存到MongoDB

该脚本用于从湘财Tushare获取日线行情数据，并保存到MongoDB数据库中
该版本通过分时间段获取和多WAN接口并行处理，解决大量数据获取问题

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=27

使用方法：
    python daily_fetcher.py              # 默认使用recent模式获取最近一周的数据更新
    python daily_fetcher.py --full        # 获取完整历史数据而非默认的最近一周数据
    python daily_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python daily_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python daily_fetcher.py --start-date 20100101 --end-date 20201231  # 指定日期范围获取数据
    python daily_fetcher.py --recent      # 显式指定recent模式（最近一周数据更新，默认模式）
"""
import os
import sys
import json
import yaml
import time
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple, Union
from pathlib import Path
from loguru import logger
from concurrent.futures import ThreadPoolExecutor

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# 导入项目模块
from data_fetcher.tushare_client import TushareClient
from storage.mongodb_client import MongoDBClient
from wan_manager.port_allocator import PortAllocator

class DailyFetcher:
    """
    日线行情数据获取器
    
    该类用于从Tushare获取日线行情数据并保存到MongoDB数据库
    优化点：
    1. 支持按时间段分批获取数据，避免一次获取超过10000条数据限制
    2. 多WAN接口并行获取，提高数据获取效率
    3. 增加数据获取重试机制，提高稳定性
    4. 支持recent模式、full模式以及指定日期范围模式
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "daily.json",
        db_name: str = None,
        collection_name: str = "daily",
        verbose: bool = False,
        max_workers: int = 4,  # 并行工作线程数
        retry_count: int = 3,  # 数据获取重试次数
        retry_delay: int = 5,  # 重试延迟时间(秒)
        batch_size: int = 10000  # 每批次获取数据的最大数量，防止超过API限制
    ):
        """
        初始化日线行情数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
            max_workers: 并行工作线程数
            retry_count: 数据获取重试次数
            retry_delay: 重试延迟时间(秒)
            batch_size: 每批获取的最大记录数
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.collection_name = collection_name
        self.verbose = verbose
        self.max_workers = max_workers
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.batch_size = batch_size

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

        # 加载配置
        self.config = self._load_config()
        self.interface_config = self._load_interface_config()
        
        # 获取token和api_url - 从配置文件读取
        tushare_config = self.config.get("tushare", {})
        self.token = tushare_config.get("token")
        self.api_url = tushare_config.get("api_url")
        
        if not self.token or not self.api_url:
            logger.error("未配置Tushare API token或API URL，请检查配置文件")
            raise ValueError("未配置Tushare API token或API URL")
        
        # 从配置获取MongoDB数据库名称
        if db_name is None:
            mongodb_config = self.config.get("mongodb", {})
            self.db_name = mongodb_config.get("db_name")
            if not self.db_name:
                logger.warning("未在配置文件中找到MongoDB数据库名称，使用默认名称'tushare_data'")
                self.db_name = "tushare_data"
        else:
            self.db_name = db_name
        
        logger.info(f"MongoDB配置: 数据库名={self.db_name}, 集合名={self.collection_name}")
        
        # 初始化Tushare客户端
        self.ts_client = TushareClient(token=self.token, api_url=self.api_url)
        
        # 初始化MongoDB客户端
        self.mongo_client = MongoDBClient(
            host=self.config.get("mongodb", {}).get("host"),
            port=self.config.get("mongodb", {}).get("port"),
            username=self.config.get("mongodb", {}).get("username"),
            password=self.config.get("mongodb", {}).get("password"),
            auth_source=self.config.get("mongodb", {}).get("auth_source"),
            auth_mechanism=self.config.get("mongodb", {}).get("auth_mechanism")
        )
        
        # 初始化端口分配器
        wan_config = self.config.get("wan", {})
        if wan_config.get("enabled", False):
            # 使用现有的全局端口分配器
            from wan_manager.port_allocator import port_allocator
            self.port_allocator = port_allocator
            wan_count = len(self.port_allocator.get_available_wan_indices())
            logger.info(f"已启用多WAN接口支持，WAN接口数量: {wan_count}")
        else:
            self.port_allocator = None
            logger.debug("未启用多WAN接口支持")
            
    def _load_config(self) -> Dict:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.debug(f"成功加载配置文件: {self.config_path}")
                return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            return {}
            
    def _load_interface_config(self) -> Dict:
        """加载接口配置文件"""
        try:
            interface_path = os.path.join(self.interface_dir, self.interface_name)
            if not os.path.exists(interface_path):
                logger.error(f"接口配置文件不存在: {interface_path}")
                return {}
                
            with open(interface_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                logger.debug(f"成功加载接口配置: {interface_path}")
                return config
        except Exception as e:
            logger.error(f"加载接口配置失败: {str(e)}")
            return {}
            
    def _generate_date_ranges(self, start_date: str, end_date: str, interval_days: int = 365) -> List[Tuple[str, str]]:
        """
        生成日期范围列表，将长时间段按interval_days天分割成多个短时间段
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            interval_days: 每个时间段的天数
            
        Returns:
            日期范围列表，每个元素为(开始日期, 结束日期)的元组
        """
        try:
            start_date_obj = datetime.strptime(start_date, '%Y%m%d')
            end_date_obj = datetime.strptime(end_date, '%Y%m%d')
            
            if start_date_obj > end_date_obj:
                logger.error(f"开始日期 {start_date} 晚于结束日期 {end_date}，将交换这两个日期")
                start_date_obj, end_date_obj = end_date_obj, start_date_obj
                
            date_ranges = []
            current_start = start_date_obj
            
            while current_start <= end_date_obj:
                current_end = current_start + timedelta(days=interval_days)
                if current_end > end_date_obj:
                    current_end = end_date_obj
                    
                date_ranges.append(
                    (current_start.strftime('%Y%m%d'), current_end.strftime('%Y%m%d'))
                )
                
                current_start = current_end + timedelta(days=1)
                if current_start > end_date_obj:
                    break
                    
            return date_ranges
        except Exception as e:
            logger.error(f"生成日期范围时出错: {str(e)}")
            return []
            
    def _get_all_stock_codes(self) -> List[str]:
        """
        获取所有股票代码
        
        Returns:
            股票代码列表
        """
        try:
            # 使用stock_basic接口获取所有股票列表
            df = self.ts_client.get_data(
                api_name="stock_basic",
                params={},
                fields=["ts_code"]
            )
            
            if df.empty:
                logger.error("获取股票列表失败，API返回数据为空")
                return []
                
            return df['ts_code'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []
            
    def _split_stock_codes(self, stock_codes: List[str], batch_size: int = 100) -> List[List[str]]:
        """
        将股票代码列表分批处理
        
        Args:
            stock_codes: 股票代码列表
            batch_size: 每批处理的股票代码数量
            
        Returns:
            股票代码批次列表
        """
        return [stock_codes[i:i+batch_size] for i in range(0, len(stock_codes), batch_size)]

    def fetch_daily_by_date(self, trade_date: str, ts_code: str = None) -> pd.DataFrame:
        """
        按日期获取日线行情数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            df = self.ts_client.get_data(
                api_name="daily",
                params=params,
                fields=self.interface_config.get("available_fields", [])
            )
            
            if df.empty:
                logger.warning(f"日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                return pd.DataFrame()
            
            logger.info(f"成功获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {len(df)} 条记录"))
            return df
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的日线数据失败: {str(e)}")
            return pd.DataFrame()
            
    def fetch_daily_by_code(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        按股票代码获取日线行情数据
        
        Args:
            ts_code: 股票代码
            start_date: 可选，开始日期，格式为YYYYMMDD
            end_date: 可选，结束日期，格式为YYYYMMDD
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            logger.debug(f"获取股票 {ts_code} 的日线数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
            
            df = self.ts_client.get_data(
                api_name="daily",
                params=params,
                fields=self.interface_config.get("available_fields", [])
            )
            
            if df.empty:
                logger.warning(f"股票 {ts_code} 未获取到数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
                return pd.DataFrame()
                
            record_count = len(df)
            
            # 检查返回的数据量是否接近限制，如果是则可能数据不完整
            if record_count >= self.batch_size * 0.9:  # 如果返回的数据量超过批次大小的90%
                logger.warning(f"股票 {ts_code} 返回数据量 {record_count} 接近API限制，数据可能不完整，建议缩小时间范围")
            
            logger.info(f"成功获取股票 {ts_code} 的日线数据，共 {record_count} 条记录")
            return df
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 的日线数据失败: {str(e)}")
            return pd.DataFrame()
    
    def fetch_data_for_date_range(self, start_date: str, end_date: str, ts_code: str = None, use_wan: bool = False, wan_index: int = None) -> pd.DataFrame:
        """
        获取指定日期范围内的日线数据
        如果日期范围内的交易日超过10000天，则按股票代码分批获取
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            use_wan: 是否使用WAN接口
            wan_index: WAN接口索引
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            # 计算日期范围内的交易日数，估算数据量
            days_diff = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days + 1
            
            # 如果已指定股票代码，则直接获取该股票的数据
            if ts_code:
                return self.fetch_daily_by_code(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            # 如果交易日可能超过10000天，按股票代码分批获取
            if days_diff > 50:  # 假设平均每年250个交易日，10000/250=40年，保守估计为50天
                logger.info(f"日期范围 {start_date} 至 {end_date} 可能包含大量交易日，将按股票代码分批获取")
                
                # 获取所有股票代码
                stock_codes = self._get_all_stock_codes()
                if not stock_codes:
                    logger.error("获取股票列表失败，无法继续获取数据")
                    return pd.DataFrame()
                    
                # 分批处理股票代码
                code_batches = self._split_stock_codes(stock_codes, batch_size=50)  # 每批处理50只股票
                all_data = []
                
                # 逐批获取数据
                for i, batch in enumerate(code_batches):
                    logger.info(f"正在获取第 {i+1}/{len(code_batches)} 批股票数据，共 {len(batch)} 只股票")
                    
                    batch_data = []
                    for code in batch:
                        df = self.fetch_daily_by_code(ts_code=code, start_date=start_date, end_date=end_date)
                        if not df.empty:
                            batch_data.append(df)
                            
                    if batch_data:
                        all_data.extend(batch_data)
                        
                # 合并所有数据
                if all_data:
                    return pd.concat(all_data, ignore_index=True)
                else:
                    return pd.DataFrame()
            else:
                # 逐日获取数据
                date_ranges = self._generate_date_ranges(start_date, end_date, interval_days=30)  # 每次获取30天数据
                all_data = []
                
                for start, end in date_ranges:
                    logger.info(f"获取日期范围 {start} 至 {end} 的日线数据")
                    daily_data = []
                    
                    current_date = datetime.strptime(start, '%Y%m%d')
                    end_date_obj = datetime.strptime(end, '%Y%m%d')
                    
                    while current_date <= end_date_obj:
                        date_str = current_date.strftime('%Y%m%d')
                        df = self.fetch_daily_by_date(trade_date=date_str, ts_code=ts_code)
                        if not df.empty:
                            daily_data.append(df)
                            
                        current_date += timedelta(days=1)
                        
                    if daily_data:
                        all_data.extend(daily_data)
                    
                # 合并所有数据
                if all_data:
                    return pd.concat(all_data, ignore_index=True)
                else:
                    return pd.DataFrame()
        except Exception as e:
            logger.error(f"获取日期范围 {start_date} 至 {end_date} 的日线数据失败: {str(e)}")
            return pd.DataFrame()

    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将日线数据保存到MongoDB
        
        Args:
            df: DataFrame形式的日线数据
            
        Returns:
            是否成功保存数据
        """
        if df.empty:
            logger.warning("没有数据需要保存到MongoDB")
            return False
            
        try:
            start_time = time.time()
            
            # 获取MongoDB集合
            db = self.mongo_client.get_database(self.db_name)
            collection = db[self.collection_name]
            
            # 转换DataFrame为字典列表
            records = df.to_dict('records')
            
            # 统计计数器
            inserted_count = 0
            skipped_count = 0
            
            # 逐条插入数据，避免重复
            for record in records:
                # 检查记录是否已存在，使用ts_code和trade_date作为复合主键
                existing_record = collection.find_one({
                    "ts_code": record["ts_code"],
                    "trade_date": record["trade_date"]
                })
                
                if existing_record:
                    # 记录已存在，跳过
                    skipped_count += 1
                    continue
                    
                # 记录不存在，插入新记录
                collection.insert_one(record)
                inserted_count += 1
            
            elapsed = time.time() - start_time
            
            # 创建索引 - 使用接口配置的index_fields
            try:
                # 根据接口配置中的index_fields创建索引
                index_fields = self.interface_config.get("index_fields", [])
                if index_fields:
                    for field in index_fields:
                        # 为字段创建索引
                        collection.create_index([(field, 1)])
                        logger.debug(f"已为字段 {field} 创建索引")
                        
                    # 为ts_code和trade_date创建复合唯一索引作为主键
                    if "ts_code" in index_fields and "trade_date" in index_fields:
                        # 检查索引是否已存在
                        existing_indexes = collection.index_information()
                        if "ts_code_1_trade_date_1" not in existing_indexes:
                            # 创建复合唯一索引
                            collection.create_index(
                                [("ts_code", 1), ("trade_date", 1)], 
                                unique=True, 
                                background=True
                            )
                            logger.debug(f"已为字段组合 (ts_code, trade_date) 创建唯一复合索引")
                else:
                    # 默认为ts_code创建索引
                    collection.create_index("ts_code")
                    logger.debug("已为默认字段ts_code创建索引")
                    
                    # 默认为trade_date创建索引
                    collection.create_index("trade_date")
                    logger.debug("已为默认字段trade_date创建索引")
            except Exception as e:
                logger.warning(f"创建索引时出错: {str(e)}")
            
            total_processed = inserted_count + skipped_count
            logger.success(f"数据处理完成: 新插入 {inserted_count} 条记录，跳过 {skipped_count} 条重复记录，共处理 {total_processed} 条记录，耗时 {elapsed:.2f}s")
            return inserted_count > 0 or skipped_count > 0
                
        except Exception as e:
            logger.error(f"保存数据到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

def main():
    """主函数"""
    import argparse
    from datetime import datetime, timedelta
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="日线行情数据获取工具")
    parser.add_argument("--verbose", action="store_true", help="输出详细日志")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据模式（API不可用时）")
    parser.add_argument("--start-date", type=str, help="开始日期，格式为YYYYMMDD，如20100101")
    parser.add_argument("--end-date", type=str, help="结束日期，格式为YYYYMMDD，如20201231")
    parser.add_argument("--recent", action="store_true", help="仅获取最近一周的数据更新（默认模式）")
    parser.add_argument("--full", action="store_true", help="获取完整历史数据而非默认的最近一周数据")
    parser.add_argument("--ts-code", type=str, help="指定股票代码，例如600000.SH")
    parser.add_argument("--batch-size", type=int, default=10000, help="每批次获取数据的最大记录数量，默认10000")
    args = parser.parse_args()
    
    # 创建获取器
    fetcher = DailyFetcher(verbose=args.verbose, batch_size=args.batch_size)
    
    # 获取日线行情数据
    if args.mock:
        logger.warning("使用模拟数据模式，生成随机的日线行情数据")
        # 使用mock数据的逻辑（暂未实现）
    else:
        # 设置日期范围
        start_date = args.start_date
        end_date = args.end_date
        
        # 如果没有指定日期范围且未设置full参数，默认使用recent模式（最近一周）
        if (not start_date or not end_date) and not args.full:
            # 使用recent模式（无论--recent是否被明确指定）
            today = datetime.now()
            end_date = today.strftime('%Y%m%d')  # 今天
            start_date = (today - timedelta(days=7)).strftime('%Y%m%d')  # 一周前
            logger.info(f"使用recent模式：获取最近一周 {start_date} 至 {end_date} 期间的数据更新")

        # 如果指定了full模式，获取全量数据（根据API能力）
        elif args.full:
            start_date = "19901219"  # A股市场最早交易日（上证）
            end_date = datetime.now().strftime('%Y%m%d')  # 今天
            logger.info(f"使用full模式：获取 {start_date} 至 {end_date} 期间的全部数据")

        # 如果指定了股票代码，优先使用股票代码
        ts_code = args.ts_code
        
        # 如果指定了日期范围，使用按日期获取数据的方式
        if start_date and end_date:
            logger.info(f"开始获取日线行情数据，日期范围: {start_date} 至 {end_date}"+(f"，股票代码: {ts_code}" if ts_code else ""))
            
            # 获取数据
            df = fetcher.fetch_data_for_date_range(
                start_date=start_date,
                end_date=end_date,
                ts_code=ts_code
            )
            
            if not df.empty:
                # 保存数据到MongoDB
                logger.info(f"正在将 {len(df)} 条日线行情数据保存到MongoDB...")
                success = fetcher.save_to_mongodb(df)
                
                if success:
                    logger.success("日线行情数据已成功保存到MongoDB")
                else:
                    logger.error("保存日线行情数据到MongoDB失败")
            else:
                logger.warning("未获取到任何日线行情数据，无需保存")
        else:
            logger.error("未指定有效的日期范围或模式，无法继续获取数据")

if __name__ == "__main__":
    main()
