#!/usr/bin/env python
"""
高管薪酬和持股数据获取器 - 获取高管薪酬和持股数据并保存到MongoDB

该脚本用于从湘财Tushare获取上市公司高管薪酬和持股数据，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票
该版本通过分时间段获取和多WAN接口并行处理，解决大量数据获取问题

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=124

使用方法：
    python stk_rewards_fetcher.py              # 默认使用recent模式获取最近一周的数据更新
    python stk_rewards_fetcher.py --full        # 获取完整历史数据而非默认的最近一周数据
    python stk_rewards_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stk_rewards_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python stk_rewards_fetcher.py --start-date 20100101 --end-date 20201231  # 指定日期范围获取数据
    python stk_rewards_fetcher.py --recent      # 显式指定recent模式（最近一周数据更新，默认模式）
"""
import os
import sys
import json
import time
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple, Union
from pathlib import Path
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
import argparse
import random

# 设置日志格式
logger.remove()
logger.add(sys.stderr, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}", level="INFO")

# 检查是否使用mock模式，如果使用则不导入可能不可用的模块
is_mock = "--mock" in sys.argv

if not is_mock:
    try:
        # 导入项目模块
        from config.config_manager import config_manager
        from data_fetcher.tushare_client import tushare_client
        from storage.mongodb_client import mongodb_client
        from wan_manager.port_allocator import PortAllocator
    except ImportError as e:
        logger.error(f"导入模块失败: {e}")
        logger.error("请确保正确安装了所有依赖并且配置文件存在，或使用 --mock 模式进行测试")
        sys.exit(1)

class StockRewardsFetcher:
    """
    高管薪酬和持股数据获取器
    
    该类用于从Tushare获取上市公司高管薪酬和持股数据并保存到MongoDB数据库，支持按市场代码过滤
    优化点：
    1. 支持按时间段分批获取数据，避免一次获取超过10000条数据限制
    2. 多WAN接口并行获取，提高数据获取效率
    3. 增加数据获取重试机制，提高稳定性
    """

    def __init__(
        self,
        interface_name: str = "stk_rewards.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},  # 默认只保存00 30 60 68四个板块的股票数据
        collection_name: str = "stk_rewards",
        verbose: bool = False,
        max_workers: int = 4,  # 并行工作线程数
        retry_count: int = 3,  # 数据获取重试次数
        retry_delay: int = 5,  # 重试延迟时间(秒)
        batch_size: int = 10000,  # 每批次获取数据的最大数量，防止超过API限制
        mock_mode: bool = False  # 是否使用模拟数据模式
    ):
        """
        初始化高管薪酬和持股数据获取器
        
        Args:
            interface_name: 接口名称
            target_market_codes: 目标市场代码集合
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
            max_workers: 并行工作线程数
            retry_count: 数据获取重试次数
            retry_delay: 重试延迟时间(秒)
            batch_size: 每批次获取数据的最大数量
            mock_mode: 是否使用模拟数据模式
        """
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.db_name = "tushare_data"  # 强制使用tushare_data作为数据库名
        self.collection_name = collection_name
        self.verbose = verbose
        self.max_workers = max_workers
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.batch_size = batch_size
        
        # 模拟数据模式标志
        self.mock_mode = mock_mode

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
        
        if self.mock_mode:
            logger.warning("已启用模拟数据模式，将使用随机生成的模拟数据")
        else:
            # 使用项目的配置管理器和客户端
            try:
                self.config = config_manager.config
                self.interface_config = self._load_interface_config()
                self.tushare_client = tushare_client
                self.mongodb_client = mongodb_client
                # 初始化WAN端口分配器
                self.port_allocator = PortAllocator()
            except Exception as e:
                logger.error(f"初始化失败: {e}")
                raise

    def _load_interface_config(self) -> Dict:
        """
        加载接口配置文件
        
        Returns:
            接口配置字典
        """
        try:
            interface_path = os.path.join(config_manager.interface_dir, self.interface_name)
            with open(interface_path, 'r', encoding='utf-8') as f:
                interface_config = json.load(f)
                return interface_config
        except Exception as e:
            logger.error(f"加载接口配置文件失败: {e}")
            sys.exit(1)

    def enable_mock_mode(self):
        """启用模拟数据模式，用于API不可用时测试"""
        self.mock_mode = True
        logger.warning("已启用模拟数据模式，将返回随机生成的假数据")

    def get_mock_data(self, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        生成模拟数据，用于API不可用时测试
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            模拟数据DataFrame
        """
        # 生成随机日期序列
        if start_date is None:
            start_date = "20180101"
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
            
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")
        
        # 随机生成条目数
        n = random.randint(5, 50)
        
        # 生成随机数据
        data = {
            "ts_code": [ts_code or f"{''.join(random.choices(['0', '3', '6', '8'], weights=[1, 1, 3, 1]))}{random.randint(0, 9)}{random.randint(0, 9)}{''.join([str(random.randint(0, 9)) for _ in range(3)])}.{'SZ' if random.random() < 0.4 else 'SH'}" for _ in range(n)],
            "ann_date": [self._random_date(start, end).strftime("%Y%m%d") for _ in range(n)],
            "end_date": [self._random_date(start, end).strftime("%Y%m%d") for _ in range(n)],
            "name": [f"高管{i}" for i in range(n)],
            "title": [random.choice(["董事长", "总经理", "财务总监", "董事", "监事", "独立董事", "副总经理"]) for _ in range(n)],
            "reward": [round(random.uniform(50, 500), 2) for _ in range(n)],
            "hold_vol": [int(random.uniform(0, 1000000)) for _ in range(n)],
        }
        
        # 构建DataFrame
        df = pd.DataFrame(data)
        return df

    def _random_date(self, start: datetime, end: datetime) -> datetime:
        """生成两个日期之间的随机日期"""
        delta = end - start
        int_delta = delta.days
        if int_delta <= 0:
            return start
        random_days = random.randint(0, int_delta)
        return start + timedelta(days=random_days)

    def get_target_ts_codes_from_stock_basic(self) -> Set[str]:
        """
        从stock_basic集合获取目标市场股票代码集合
        
        Returns:
            股票代码集合
        """
        try:
            # 连接MongoDB
            collection = self.mongodb_client.get_collection(self.db_name, "stock_basic")
            
            # 构建查询条件，只获取目标市场的股票
            query = {"ts_code": {"$regex": r"^\d{6}\.(SH|SZ)$"}}
            
            # 投影，只获取ts_code字段
            projection = {"ts_code": 1, "_id": 0}
            
            # 查询数据
            cursor = collection.find(query, projection)
            
            # 过滤出目标市场的股票
            result = set()
            for doc in cursor:
                ts_code = doc.get("ts_code", "")
                if ts_code and len(ts_code) >= 8:
                    market_code = ts_code[:2]
                    if market_code in self.target_market_codes:
                        result.add(ts_code)
            
            logger.info(f"从stock_basic获取到{len(result)}个符合条件的股票代码")
            return result
        except Exception as e:
            logger.error(f"从MongoDB获取股票代码失败: {e}")
            return set()

    def fetch_rewards_by_code(self, ts_code: str, wan_idx: int = None, port: int = None) -> Optional[pd.DataFrame]:
        """
        按股票代码获取高管薪酬和持股数据
        
        Args:
            ts_code: 股票代码
            wan_idx: WAN接口索引，如果为None则获取新的WAN接口
            port: WAN接口端口，如果为None则分配新的端口
            
        Returns:
            高管薪酬和持股数据DataFrame，如果失败则返回None
        """
        # 添加重试机制
        for retry in range(self.retry_count + 1):
            try:
                # 准备参数
                params = {
                    "ts_code": ts_code
                }
                
                # 如果是模拟模式，返回模拟数据
                if self.mock_mode:
                    return self.get_mock_data(ts_code=ts_code)
                
                # 使用WAN接口
                if wan_idx is None or port is None:
                    wan_idx, port = self.port_allocator.get_next_port()
                    
                # 调用Tushare API
                df = self.tushare_client.query_data(
                    api_name=self.interface_config["api_name"],
                    params=params,
                    fields=self.interface_config["available_fields"],
                    wan_idx=wan_idx,
                    port=port
                )
                
                if df is not None and not df.empty:
                    logger.info(f"成功获取到股票{ts_code}的高管薪酬和持股数据，共{len(df)}条记录")
                    return df
                else:
                    logger.warning(f"未获取到股票{ts_code}的高管薪酬和持股数据")
                    return None
            except Exception as e:
                if retry < self.retry_count:
                    logger.warning(f"获取股票{ts_code}高管薪酬和持股数据失败，重试第{retry+1}次: {e}")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"获取股票{ts_code}高管薪酬和持股数据失败: {e}")
                    return None

    def fetch_rewards_by_date(self, start_date: str, end_date: str, ts_code: str = None, wan_idx: int = None, port: int = None, batch_size: int = 10000) -> Optional[pd.DataFrame]:
        """
        按公告日期范围获取高管薪酬和持股数据，支持分批获取以避免超过API限制
        
        Args:
            start_date: 开始日期，格式为 YYYYMMDD
            end_date: 结束日期，格式为 YYYYMMDD
            ts_code: 股票代码，可选，如果提供则只获取该股票的数据
            wan_idx: WAN接口索引，如果为None则获取新的WAN接口
            port: WAN接口端口，如果为None则分配新的端口
            batch_size: 每批获取的最大记录数，防止超过API限制
            
        Returns:
            高管薪酬和持股数据DataFrame，如果失败则返回None
        """
        # 添加重试机制
        for retry in range(self.retry_count + 1):
            try:
                # 准备参数
                params = {
                    "ann_date_start": start_date,
                    "ann_date_end": end_date
                }
                
                # 如果提供了股票代码，则添加到参数中
                if ts_code is not None:
                    params["ts_code"] = ts_code
                
                # 如果是模拟模式，返回模拟数据
                if self.mock_mode:
                    return self.get_mock_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                # 使用WAN接口
                if wan_idx is None or port is None:
                    wan_idx, port = self.port_allocator.get_next_port()
                    
                # 调用Tushare API
                df = self.tushare_client.query_data(
                    api_name=self.interface_config["api_name"],
                    params=params,
                    fields=self.interface_config["available_fields"],
                    wan_idx=wan_idx,
                    port=port
                )
                
                if df is not None and not df.empty:
                    date_str = f"{start_date}至{end_date}"
                    if ts_code:
                        logger.info(f"成功获取到股票{ts_code}在{date_str}期间的高管薪酬和持股数据，共{len(df)}条记录")
                    else:
                        logger.info(f"成功获取到{date_str}期间的高管薪酬和持股数据，共{len(df)}条记录")
                    return df
                else:
                    date_str = f"{start_date}至{end_date}"
                    if ts_code:
                        logger.warning(f"未获取到股票{ts_code}在{date_str}期间的高管薪酬和持股数据")
                    else:
                        logger.warning(f"未获取到{date_str}期间的高管薪酬和持股数据")
                    return None
            except Exception as e:
                if retry < self.retry_count:
                    logger.warning(f"获取{start_date}至{end_date}期间高管薪酬和持股数据失败，重试第{retry+1}次: {e}")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"获取{start_date}至{end_date}期间高管薪酬和持股数据失败: {e}")
                    return None

    def fetch_rewards(self, ts_codes: Set[str] = None, start_date=None, end_date=None) -> Optional[pd.DataFrame]:
        """
        获取高管薪酬和持股数据
        
        Args:
            ts_codes: 股票代码集合，如果为None则从stock_basic集合获取
            start_date: 开始日期，格式为 YYYYMMDD，用于按日期范围过滤
            end_date: 结束日期，格式为 YYYYMMDD，用于按日期范围过滤
        
        Returns:
            高管薪酬和持股数据DataFrame，如果失败则返回None
        """
        try:
            # 如果未提供股票代码集合，从stock_basic获取
            if ts_codes is None:
                ts_codes = self.get_target_ts_codes_from_stock_basic()
            
            if not ts_codes:
                logger.error("未能获取到有效的股票代码列表，无法继续获取数据")
                return None
                
            # 如果提供了日期范围，按日期范围获取数据
            if start_date and end_date:
                logger.info(f"按日期范围{start_date}至{end_date}获取高管薪酬和持股数据")
                return self.fetch_rewards_by_date(start_date, end_date)
                
            # 否则，按股票代码批量获取数据
            logger.info(f"开始按股票代码批量获取高管薪酬和持股数据，共{len(ts_codes)}个股票")
            all_results = []
            
            # 使用多线程并行获取数据
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 分配WAN接口
                wan_ports = [self.port_allocator.get_next_port() for _ in range(self.max_workers)]
                
                # 创建任务
                future_to_ts_code = {}
                for i, ts_code in enumerate(ts_codes):
                    # 循环使用WAN接口
                    wan_idx, port = wan_ports[i % self.max_workers]
                    future = executor.submit(self.fetch_rewards_by_code, ts_code, wan_idx, port)
                    future_to_ts_code[future] = ts_code
                
                # 收集结果
                for future in future_to_ts_code:
                    ts_code = future_to_ts_code[future]
                    try:
                        df = future.result()
                        if df is not None and not df.empty:
                            all_results.append(df)
                    except Exception as e:
                        logger.error(f"处理股票{ts_code}的高管薪酬和持股数据时出错: {e}")
            
            # 合并所有结果
            if all_results:
                result_df = pd.concat(all_results, ignore_index=True)
                logger.info(f"成功获取高管薪酬和持股数据，共{len(result_df)}条记录")
                return result_df
            else:
                logger.warning("未获取到有效的高管薪酬和持股数据")
                return None
                
        except Exception as e:
            logger.error(f"获取高管薪酬和持股数据过程中发生错误: {e}")
            return None

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理数据，包括数据类型转换、缺失值处理等
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            处理后的DataFrame
        """
        if df is None or df.empty:
            return df
            
        try:
            # 复制数据避免修改原始数据
            result = df.copy()
            
            # 处理数值型字段
            if "reward" in result.columns:
                result["reward"] = pd.to_numeric(result["reward"], errors='coerce')
                
            if "hold_vol" in result.columns:
                result["hold_vol"] = pd.to_numeric(result["hold_vol"], errors='coerce')
                
            # 处理日期型字段
            date_columns = ["ann_date", "end_date"]
            for col in date_columns:
                if col in result.columns:
                    # 确保日期格式统一为YYYYMMDD
                    result[col] = result[col].astype(str).str.replace('-', '')
            
            # 处理缺失值
            result = result.replace([np.inf, -np.inf], np.nan)
            
            # 添加更新时间字段
            result["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return result
            
        except Exception as e:
            logger.error(f"处理数据时出错: {e}")
            return df

    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将数据保存到MongoDB
        
        Args:
            df: 数据DataFrame
            
        Returns:
            是否保存成功
        """
        if df is None or df.empty:
            logger.warning("没有数据需要保存")
            return False
            
        try:
            # 获取MongoDB集合
            collection = self.mongodb_client.get_collection(self.db_name, self.collection_name)
            
            # 将DataFrame转换为字典列表
            data = df.to_dict('records')
            
            # 构建索引以提高查询效率
            index_fields = self.interface_config.get("index_fields", [])
            if index_fields:
                for field in index_fields:
                    if field in df.columns:
                        collection.create_index(field)
                
                # 创建复合索引
                collection.create_index([(field, 1) for field in index_fields if field in df.columns])
            
            # 批量保存数据，使用upsert模式避免重复
            for record in data:
                # 构建查询条件
                query = {}
                for field in ["ts_code", "end_date", "ann_date", "name", "title"]:
                    if field in record and record[field] is not None:
                        query[field] = record[field]
                
                # 如果查询条件为空，则直接插入
                if not query:
                    collection.insert_one(record)
                else:
                    # 使用upsert模式更新或插入
                    collection.update_one(query, {"$set": record}, upsert=True)
            
            logger.info(f"成功保存{len(data)}条高管薪酬和持股数据到MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"保存数据到MongoDB时出错: {e}")
            return False

    def run(self, mode: str = "recent", start_date: str = None, end_date: str = None, mock: bool = False) -> bool:
        """
        执行数据获取和保存
        
        Args:
            mode: 数据获取模式，可选值：recent(默认，最近一周), full(全量历史数据)
            start_date: 指定开始日期，格式为YYYYMMDD
            end_date: 指定结束日期，格式为YYYYMMDD
            mock: 是否启用模拟数据模式
            
        Returns:
            是否执行成功
        """
        try:
            # 如果启用模拟模式
            if mock:
                self.enable_mock_mode()
            
            # 确定日期范围
            if mode == "full":
                # 全量历史数据模式，获取从2000年至今的数据
                if not start_date:
                    start_date = "20000101"
                if not end_date:
                    end_date = datetime.now().strftime("%Y%m%d")
                    
                logger.info(f"启动全量历史数据获取模式，日期范围: {start_date} 至 {end_date}")
            
            elif mode == "recent":
                # 最近一周数据更新模式
                if not end_date:
                    end_date = datetime.now().strftime("%Y%m%d")
                if not start_date:
                    # 计算一周前的日期
                    start = datetime.now() - timedelta(days=7)
                    start_date = start.strftime("%Y%m%d")
                    
                logger.info(f"启动最近数据更新模式，日期范围: {start_date} 至 {end_date}")
            
            else:
                # 自定义日期范围模式
                if not start_date or not end_date:
                    logger.error("自定义模式下必须同时提供开始日期和结束日期")
                    return False
                    
                logger.info(f"启动自定义日期范围模式: {start_date} 至 {end_date}")
            
            # 获取数据
            df = self.fetch_rewards(start_date=start_date, end_date=end_date)
            
            # 处理数据
            if df is not None and not df.empty:
                df = self.process_data(df)
                
                # 保存数据
                return self.save_to_mongodb(df)
            else:
                logger.warning("未获取到有效数据，无需保存")
                return False
                
        except Exception as e:
            logger.error(f"执行数据获取和保存过程中发生错误: {e}")
            return False

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='高管薪酬和持股数据获取工具')
    
    # 数据获取模式参数
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--full', action='store_true', help='获取全量历史数据')
    mode_group.add_argument('--recent', action='store_true', help='获取最近一周数据更新（默认模式）')
    
    # 日期范围参数
    parser.add_argument('--start-date', type=str, help='指定开始日期，格式为YYYYMMDD')
    parser.add_argument('--end-date', type=str, help='指定结束日期，格式为YYYYMMDD')
    
    # 其他参数
    parser.add_argument('--verbose', action='store_true', help='启用详细日志输出')
    parser.add_argument('--mock', action='store_true', help='使用模拟数据模式（API不可用时）')
    
    return parser.parse_args()

def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()
    
    # 确定运行模式
    mode = "recent"  # 默认模式
    if args.full:
        mode = "full"
    
    # 初始化获取器
    try:
        fetcher = StockRewardsFetcher(verbose=args.verbose, mock_mode=args.mock)
        
        # 执行数据获取和保存
        if args.mock:
            # 如果是模拟模式，只获取数据并打印，不保存到数据库
            logger.info("模拟模式：获取模拟数据并打印，不保存到数据库")
            df = fetcher.get_mock_data()
            if df is not None and not df.empty:
                logger.info(f"成功生成模拟数据，共{len(df)}条记录")
                logger.info("\n" + df.head(10).to_string())
                logger.success("模拟数据生成成功")
                return 0
            else:
                logger.error("模拟数据生成失败")
                return 1
        else:
            # 正常模式：获取数据并保存到数据库
            success = fetcher.run(
                mode=mode,
                start_date=args.start_date,
                end_date=args.end_date
            )
            
            # 输出结果
            if success:
                logger.success("高管薪酬和持股数据获取和保存成功完成")
                return 0
            else:
                logger.error("高管薪酬和持股数据获取或保存过程失败")
                return 1
    except Exception as e:
        logger.error(f"运行过程中发生错误: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())