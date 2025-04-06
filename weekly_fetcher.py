#!/usr/bin/env python
"""
周线行情数据获取器 - 获取周线行情数据并保存到MongoDB

该脚本用于从湘财Tushare获取周线行情数据，并保存到MongoDB数据库中
该版本通过分时间段获取和多WAN接口并行处理，解决大量数据获取问题

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=10144

使用方法：
    python weekly_fetcher.py              # 默认使用recent模式获取最近几周的数据更新
    python weekly_fetcher.py --full        # 获取完整历史数据而非默认的最近几周数据
    python weekly_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python weekly_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python weekly_fetcher.py --start-date 20100101 --end-date 20201231  # 指定日期范围获取数据
    python weekly_fetcher.py --recent      # 显式指定recent模式（最近几周数据更新，默认模式）
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
import pymongo
import requests.adapters
import socket
import requests
import random
import queue
import threading

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))


# 导入项目模块
from data_fetcher.tushare_client import TushareClient
from storage.mongodb_client import MongoDBClient
from wan_manager.port_allocator import PortAllocator

class TushareClientWAN:
    """
    专用于WAN绑定的Tushare客户端
    """
    def __init__(self, token: str, local_ip: str = None):
        """
        初始化绑定指定WAN的Tushare客户端
        """
        self.token = token
        self.local_ip = local_ip
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=100
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        if local_ip:
            self.setup_source_address(local_ip)

    def setup_source_address(self, local_ip: str):
        """
        配置源地址绑定
        """
        original_create_connection = socket.create_connection

        def patched_create_connection(address, *args, **kwargs):
            host, port = address
            kwargs['source_address'] = (local_ip, 0)
            return original_create_connection(address, *args, **kwargs)

        socket.create_connection = patched_create_connection

    async def query_weekly(self, ts_code: str = None, trade_date: str = None, 
                         start_date: str = None, end_date: str = None,
                         fields: List[str] = None) -> pd.DataFrame:
        """
        查询周线行情数据
        """
        api_name = 'weekly'
        params = {}
        if ts_code:
            params['ts_code'] = ts_code
        if trade_date:
            params['trade_date'] = trade_date
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date

        url = "http://116.128.206.39:7172/api"
        data = {
            'api_name': api_name,
            'token': self.token,
            'params': params,
            'fields': fields
        }

        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.session.post(url, json=data, timeout=30))
            if response.status_code != 200:
                logger.error(f"API请求失败: {response.status_code} {response.text}")
                return pd.DataFrame()

            result = response.json()
            if result['code'] != 0:
                logger.warning(f"API返回错误: {result['code']} {result['msg']}")
                return pd.DataFrame()

            data = result.get('data', {})
            if not data:
                logger.info(f"API返回空数据")
                return pd.DataFrame()

            items = data.get('items', [])
            if not items:
                logger.info(f"API返回空记录")
                return pd.DataFrame()

            df = pd.DataFrame(items)
            return df
        except Exception as e:
            logger.exception(f"查询周线数据异常: {e}")
            return pd.DataFrame()

class WeeklyDataFetcher:
    """
    周线行情数据获取器
    """
    def __init__(self, config_path: str = "config/config.yaml", verbose: bool = False, 
                 mock: bool = False, use_parallel: bool = True):
        """
        初始化周线数据获取器
        """
        self.verbose = verbose
        self.mock = mock
        self.use_parallel = use_parallel
        self.config = self._load_config(config_path)
        self.interface_config = self._load_interface_config("config/interfaces/weekly.json")
        self.mongodb_client = MongoDBClient(
            host=self.config.get('mongodb', {}).get('host', 'localhost'),
            port=self.config.get('mongodb', {}).get('port', 27017),
            username=self.config.get('mongodb', {}).get('username'),
            password=self.config.get('mongodb', {}).get('password')
        )
        self.tushare_token = self.config.get('tushare', {}).get('token')
        self.available_fields = self.interface_config.get('available_fields', [])
        self.collection_name = "weekly"
        self.lock = threading.Lock()
        self.port_allocator = PortAllocator()
        
        if verbose:
            logger.add(sys.stderr, level="DEBUG")
        else:
            logger.add(sys.stderr, level="INFO")


    def _load_config(self, config_path: str) -> Dict:
        """
        加载配置文件
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            return {}

    def _load_interface_config(self, config_path: str) -> Dict:
        """
        加载接口配置文件
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"加载接口配置文件失败: {e}")
            return {}

    async def _get_stock_list(self) -> List[str]:
        """
        获取股票代码列表
        """
        tushare_client = TushareClient(token=self.tushare_token)
        df = await tushare_client.query('stock_basic', exchange='', list_status='L', fields=['ts_code'])
        if df.empty:
            logger.error("获取股票列表失败")
            return []
        return df['ts_code'].tolist()

    def _get_trade_cal(self, start_date: str, end_date: str) -> List[str]:
        """
        从MongoDB中获取指定日期范围内的交易日历
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            交易日期列表，格式为YYYYMMDD
        """
        try:
            # 确保MongoDB连接
            if not hasattr(self.mongodb_client, 'is_connected') or not self.mongodb_client.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if hasattr(self.mongodb_client, 'connect') and not self.mongodb_client.connect():
                    logger.error("连接MongoDB失败")
                    return []
                    
            # 连接MongoDB
            db = self.mongodb_client.get_db('tushare_data')
            
            # 一般交易日历保存在trade_cal集合中
            collection = db["trade_cal"]
            
            # 查询指定日期范围内的交易日
            query = {
                "trade_date": {"$gte": start_date, "$lte": end_date}
                # 注意：trade_cal集合中可能没有is_open字段
            }
            
            # 只获取日期字段
            projection = {"trade_date": 1, "_id": 0}
            
            # 检查集合中是否有数据
            count = collection.count_documents({})
            logger.info(f"交易日历集合中共有 {count} 条记录")
            
            # 记录查询条件
            logger.info(f"查询交易日历: 条件={query}, 投影={projection}")
            
            # 执行查询
            cursor = collection.find(query, projection).sort("trade_date", 1)
            
            # 将结果转换为列表
            trade_dates = [doc["trade_date"] for doc in cursor]
            
            if not trade_dates:
                logger.warning(f"未找到时间范围内的交易日: {start_date}至{end_date}")
            else:
                logger.info(f"查询到 {len(trade_dates)} 个交易日: 从 {trade_dates[0]} 到 {trade_dates[-1]}")
            
            return trade_dates
            
        except Exception as e:
            logger.exception(f"获取交易日历时出错: {e}")
            return []

    def _get_recent_weeks(self, weeks: int = 4) -> Tuple[str, str]:
        """
        获取最近几周的日期范围
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=weeks * 7)
        return (
            start_date.strftime("%Y%m%d"),
            end_date.strftime("%Y%m%d")
        )
    
    def _split_list(self, lst: List, n: int) -> List[List]:
        """
        将列表分割为n个子列表
        """
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    async def _fetch_weekly_by_date(self, date: str) -> pd.DataFrame:
        """
        根据交易日期获取周线数据
        """
        # 实际场景中可能需要使用实际绑定IP地址
        tushare_client = TushareClientWAN(token=self.tushare_token)
        df = await tushare_client.query_weekly(
            trade_date=date,
            fields=self.available_fields
        )
        return df

    async def _fetch_weekly_by_ts_code(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        根据股票代码获取周线数据
        """
        # 实际场景中可能需要使用实际绑定IP地址
        tushare_client = TushareClientWAN(token=self.tushare_token)
        df = await tushare_client.query_weekly(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=self.available_fields
        )
        return df

    async def _fetch_weekly_by_date_range_for_stock(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指定股票指定日期范围的周线数据
        """
        return await self._fetch_weekly_by_ts_code(ts_code, start_date, end_date)

    async def _process_stock_batch(self, stock_batch: List[str], start_date: str, end_date: str, 
                               wan_ip: str = None) -> List[Dict]:
        """
        处理一批股票的数据获取
        """
        all_data = []
        for ts_code in stock_batch:
            try:
                # 创建绑定指定WAN的客户端
                tushare_client = TushareClientWAN(token=self.tushare_token, local_ip=wan_ip)
                
                # 获取数据
                df = await tushare_client.query_weekly(
                    ts_code=ts_code,
                    start_date=start_date,
                    end_date=end_date,
                    fields=self.available_fields
                )
                
                if not df.empty:
                    records = df.to_dict('records')
                    all_data.extend(records)
                    logger.info(f"获取股票 {ts_code} 的周线数据: {len(records)} 条记录")
                else:
                    logger.warning(f"股票 {ts_code} 的周线数据为空")
                    
                # API限流控制，每次请求后稍作延迟
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.exception(f"处理股票 {ts_code} 时出错: {e}")
                
        return all_data


    def _mock_weekly_data(self) -> List[Dict]:
        """
        生成模拟周线数据（仅用于测试）
        """
        mock_data = []
        ts_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
        trade_dates = ['20230101', '20230108', '20230115', '20230122']
        
        for ts_code in ts_codes:
            for date in trade_dates:
                # 模拟周线数据结构
                record = {
                    'ts_code': ts_code,
                    'trade_date': date,
                    'open': round(random.uniform(10, 50), 2),
                    'high': round(random.uniform(10, 50), 2),
                    'low': round(random.uniform(10, 50), 2),
                    'close': round(random.uniform(10, 50), 2),
                    'pre_close': round(random.uniform(10, 50), 2),
                    'change': round(random.uniform(-5, 5), 2),
                    'pct_chg': round(random.uniform(-10, 10), 2),
                    'vol': round(random.uniform(1000, 10000), 2),
                    'amount': round(random.uniform(10000, 100000), 2)
                }
                mock_data.append(record)
                
        return mock_data

    def _save_to_mongodb(self, data: List[Dict]) -> bool:
        """
        保存数据到MongoDB
        """
        if not data:
            logger.warning("没有数据需要保存")
            return False
            
        try:
            # 按照composite_index创建一个复合索引，用于防止重复数据
            collection = self.mongodb_client.get_collection(self.collection_name)
            collection.create_index(
                [(index_field, pymongo.ASCENDING) for index_field in self.interface_config.get('index_fields', [])],
                unique=True,
                background=True
            )
            
            # 使用bulk_write进行批量插入或更新，提高效率
            operations = []
            for record in data:
                filter_dict = {index_field: record[index_field] 
                                for index_field in self.interface_config.get('index_fields', []) 
                                if index_field in record}
                if filter_dict:
                    operations.append(
                        pymongo.UpdateOne(
                            filter_dict,
                            {'$set': record},
                            upsert=True
                        )
                    )
                    
            if operations:
                result = collection.bulk_write(operations, ordered=False)
                logger.info(f"数据保存成功: 插入 {result.upserted_count} 条, 更新 {result.modified_count} 条")
                return True
            else:
                logger.warning("未生成有效的MongoDB操作")
                return False
        except Exception as e:
            logger.exception(f"保存数据到MongoDB失败: {e}")
            return False


    async def fetch_recent_data(self):
        """
        获取最近几周的数据更新
        """
        start_date, end_date = self._get_recent_weeks(weeks=4)
        logger.info(f"获取最近几周数据: 从 {start_date} 到 {end_date}")
        
        # 获取交易日历
        trade_dates = self._get_trade_cal(start_date, end_date)
        if not trade_dates:
            logger.error("获取交易日历失败，无法继续")
            return False
            
        # 按周进行采集，避免单次请求数据过多
        for date_batch in self._split_list(trade_dates, 5):
            # 每周最后一个交易日当作周数据点
            weekly_date = date_batch[-1]
            logger.info(f"处理周数据日期: {weekly_date}")
            
            if self.mock:
                data = self._mock_weekly_data()
                logger.info(f"模拟周线数据: {len(data)} 条记录")
            else:
                df = await self._fetch_weekly_by_date(weekly_date)
                if df.empty:
                    logger.warning(f"日期 {weekly_date} 的周线数据为空")
                    continue
                    
                data = df.to_dict('records')
                logger.info(f"获取到周线数据: {len(data)} 条记录")
                
            if data:
                self._save_to_mongodb(data)
                
        return True

    async def fetch_full_history(self, batch_size: int = 1, market_codes: List[str] = None):
        """
        获取完整历史周线数据
        """
        # 默认获取从1990年至今的所有周线数据
        start_date = "19900101"
        end_date = datetime.now().strftime("%Y%m%d")
        logger.info(f"获取完整历史周线数据: 从 {start_date} 到 {end_date}")
        
        if self.mock:
            data = self._mock_weekly_data()
            self._save_to_mongodb(data)
            logger.info(f"模拟数据: {len(data)} 条记录")
            return True

            
        # 获取股票列表
        stock_list = await self._get_stock_list()
        if not stock_list:
            logger.error("获取股票列表失败，无法继续")
            return False
            
        logger.info(f"获取到股票列表: {len(stock_list)} 只股票")
        
        # 根据市场代码过滤股票
        if market_codes:
            filtered_stock_list = []
            for ts_code in stock_list:
                # 股票代码格式为: 600000.SH，取前2位为市场代码
                code_prefix = ts_code[:2]
                if code_prefix in market_codes:
                    filtered_stock_list.append(ts_code)
            stock_list = filtered_stock_list
            logger.info(f"过滤后的股票列表: {len(stock_list)} 只股票")
            
        # 按批次处理股票
        stock_batches = self._split_list(stock_list, batch_size)
        logger.info(f"分批处理股票: {len(stock_batches)} 批次，每批 {batch_size} 只股票")
        
        total_records = 0
        success_batches = 0
        
        # 获取可用的WAN接口IP
        wan_ips = self.port_allocator.get_available_ips()
        if not wan_ips and self.use_parallel:
            logger.warning("没有可用的WAN接口IP，使用默认网络连接")
        
        # 并行处理数据获取
        if self.use_parallel and wan_ips:
            logger.info(f"使用并行处理模式，{len(wan_ips)} 个WAN接口")
            
            # 创建工作队列
            task_queue = queue.Queue()
            for i, batch in enumerate(stock_batches):
                task_queue.put((i, batch))
                
            # 创建结果容器
            results = []
                
            # 定义工作线程函数
            async def worker(worker_id, worker_ip):
                worker_records = 0
                while not task_queue.empty():
                    try:
                        batch_id, stock_batch = task_queue.get_nowait()
                    except queue.Empty:
                        break
                        
                    try:
                        logger.info(f"工作线程 {worker_id} (IP: {worker_ip}) 处理批次 {batch_id}: {stock_batch}")
                        batch_data = await self._process_stock_batch(
                            stock_batch=stock_batch,
                            start_date=start_date,
                            end_date=end_date,
                            wan_ip=worker_ip
                        )
                        
                        if batch_data:
                            with self.lock:
                                results.extend(batch_data)
                                worker_records += len(batch_data)
                            
                        task_queue.task_done()
                        
                    except Exception as e:
                        logger.exception(f"工作线程 {worker_id} 处理批次 {batch_id} 时出错: {e}")
                        task_queue.task_done()
                
                return worker_records

                
            # 启动工作线程
            workers = []
            for i, ip in enumerate(wan_ips):
                workers.append(worker(i, ip))
                
            # 等待所有任务完成
            worker_results = await asyncio.gather(*workers)
            
            # 汇总结果
            for worker_records in worker_results:
                total_records += worker_records
                
            # 保存到数据库
            if results:
                success = self._save_to_mongodb(results)
                if success:
                    success_batches += 1
                    
        else:
            logger.info("使用串行处理模式")
            # 串行处理
            for i, batch in enumerate(stock_batches):
                try:
                    logger.info(f"处理批次 {i+1}/{len(stock_batches)}: {batch}")
                    batch_data = await self._process_stock_batch(
                        stock_batch=batch,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if batch_data:
                        # 批量保存数据
                        success = self._save_to_mongodb(batch_data)
                        total_records += len(batch_data)
                        
                        if success:
                            success_batches += 1
                            
                except Exception as e:
                    logger.exception(f"处理批次 {i+1} 时出错: {e}")
                    
                # 添加延迟，避免API限流
                await asyncio.sleep(1)
        
        logger.info(f"周线数据获取完成: 总共获取 {total_records} 条记录，成功批次: {success_batches}/{len(stock_batches)}")
        return True

    async def fetch_by_date_range(self, start_date: str, end_date: str, 
                              batch_size: int = 1, market_codes: List[str] = None):
        """
        按日期范围获取周线数据
        """
        logger.info(f"按日期范围获取周线数据: 从 {start_date} 到 {end_date}")
        
        if self.mock:
            data = self._mock_weekly_data()
            self._save_to_mongodb(data)
            logger.info(f"模拟数据: {len(data)} 条记录")
            return True
        
        # 获取股票列表
        stock_list = await self._get_stock_list()
        if not stock_list:
            logger.error("获取股票列表失败，无法继续")
            return False

            
        logger.info(f"获取到股票列表: {len(stock_list)} 只股票")
        
        # 根据市场代码过滤股票
        if market_codes:
            filtered_stock_list = []
            for ts_code in stock_list:
                code_prefix = ts_code[:2]
                if code_prefix in market_codes:
                    filtered_stock_list.append(ts_code)
            stock_list = filtered_stock_list
            logger.info(f"过滤后的股票列表: {len(stock_list)} 只股票")
            
        # 按批次处理股票
        stock_batches = self._split_list(stock_list, batch_size)
        logger.info(f"分批处理股票: {len(stock_batches)} 批次，每批 {batch_size} 只股票")
        
        total_records = 0
        success_batches = 0
        
        # 获取可用的WAN接口IP
        wan_ips = self.port_allocator.get_available_ips()
        if not wan_ips and self.use_parallel:
            logger.warning("没有可用的WAN接口IP，使用默认网络连接")
        
        # 并行处理数据获取
        if self.use_parallel and wan_ips:
            logger.info(f"使用并行处理模式，{len(wan_ips)} 个WAN接口")
            
            # 创建工作队列
            task_queue = queue.Queue()
            for i, batch in enumerate(stock_batches):
                task_queue.put((i, batch))
                
            # 创建结果容器
            results = []
                
            # 定义工作线程函数
            async def worker(worker_id, worker_ip):
                worker_records = 0
                while not task_queue.empty():
                    try:
                        batch_id, stock_batch = task_queue.get_nowait()
                    except queue.Empty:
                        break
                        
                    try:
                        logger.info(f"工作线程 {worker_id} (IP: {worker_ip}) 处理批次 {batch_id}: {stock_batch}")
                        batch_data = await self._process_stock_batch(
                            stock_batch=stock_batch,
                            start_date=start_date,
                            end_date=end_date,
                            wan_ip=worker_ip
                        )
                        
                        if batch_data:
                            with self.lock:
                                results.extend(batch_data)
                                worker_records += len(batch_data)
                            
                        task_queue.task_done()
                        
                    except Exception as e:
                        logger.exception(f"工作线程 {worker_id} 处理批次 {batch_id} 时出错: {e}")
                        task_queue.task_done()
                
                return worker_records
                
            # 启动工作线程
            workers = []
            for i, ip in enumerate(wan_ips):
                workers.append(worker(i, ip))
                
            # 等待所有任务完成
            worker_results = await asyncio.gather(*workers)
            
            # 汇总结果
            for worker_records in worker_results:
                total_records += worker_records
                
            # 保存到数据库
            if results:
                success = self._save_to_mongodb(results)
                if success:
                    success_batches += 1
                    
        else:
            logger.info("使用串行处理模式")
            # 串行处理
            for i, batch in enumerate(stock_batches):
                try:
                    logger.info(f"处理批次 {i+1}/{len(stock_batches)}: {batch}")
                    batch_data = await self._process_stock_batch(
                        stock_batch=batch,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if batch_data:
                        # 批量保存数据
                        success = self._save_to_mongodb(batch_data)
                        total_records += len(batch_data)
                        
                        if success:
                            success_batches += 1
                            
                except Exception as e:
                    logger.exception(f"处理批次 {i+1} 时出错: {e}")
                    
                # 添加延迟，避免API限流
                await asyncio.sleep(1)
        
        logger.info(f"周线数据获取完成: 总共获取 {total_records} 条记录，成功批次: {success_batches}/{len(stock_batches)}")
        return True


async def main():
    import argparse
    parser = argparse.ArgumentParser(description="获取周线行情数据")
    parser.add_argument("--mode", choices=["recent", "full", "range"], default="recent", help="数据获取模式")
    parser.add_argument("--start-date", help="开始日期 (YYYYMMDD)")
    parser.add_argument("--end-date", help="结束日期 (YYYYMMDD)")
    parser.add_argument("--batch-size", type=int, default=1, help="批处理大小")
    parser.add_argument("--market-codes", nargs="+", help="市场代码过滤(如: 60 00 30)")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据")
    parser.add_argument("--verbose", action="store_true", help="显示详细日志")
    parser.add_argument("--no-parallel", action="store_true", help="禁用并行处理")
    args = parser.parse_args()
    
    fetcher = WeeklyDataFetcher(
        verbose=args.verbose,
        mock=args.mock,
        use_parallel=not args.no_parallel
    )
    
    if args.mode == "recent" or (not args.start_date and not args.end_date):
        result = await fetcher.fetch_recent_data()
    elif args.mode == "full":
        result = await fetcher.fetch_full_history(batch_size=args.batch_size, market_codes=args.market_codes)
    elif args.mode == "range" and args.start_date and args.end_date:
        result = await fetcher.fetch_by_date_range(
            start_date=args.start_date,
            end_date=args.end_date,
            batch_size=args.batch_size,
            market_codes=args.market_codes
        )
    else:
        logger.error("参数错误: 范围模式需要同时指定 --start-date 和 --end-date")
        return 1
    
    if result:
        logger.info("周线数据获取成功")
        return 0
    else:
        logger.error("周线数据获取失败")
        return 1

if __name__ == "__main__":
    asyncio.run(main())
