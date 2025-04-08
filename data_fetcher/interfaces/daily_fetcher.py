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
    python daily_fetcher.py --start-date 20100101 --end-date 20201231  # 指定日期范围获取数据
    python daily_fetcher.py --recent      # 显式指定recent模式（最近一周数据更新，默认模式）
"""
import os
import sys
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path
from loguru import logger
import pymongo



# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core import BaseFetcher
from core import config_manager, mongodb_handler, network_manager
from core.wan_manager import port_allocator
from core.tushare_client_wan import TushareClientWAN

class DailyFetcher(BaseFetcher):
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
        target_market_codes: Set[str] = {"00", "30", "60", "68"},  # 默认只保存00 30 60 68四个板块的股票数据
        collection_name: str = "daily",
        interface_dir: str = "config/interfaces",
        interface_name: str = "daily.json",
        verbose: bool = False,
        max_workers: int = 3,  # 并行工作线程数
        retry_count: int = 10,  # 数据获取重试次数
        retry_delay: int = 5,  # 重试延迟时间(秒)
        batch_size: int = 10000  # 每批次获取数据的最大数量，防止超过API限制
    ):
        """
        初始化日线行情数据获取器
        
        Args:
            target_market_codes: 目标市场代码集合，只保存这些板块的股票数据
            collection_name: MongoDB集合名称
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            verbose: 是否输出详细日志
            max_workers: 并行工作线程数
            retry_count: 数据获取重试次数
            retry_delay: 重试延迟时间(秒)
            batch_size: 每批获取的最大记录数
        """
        # 调用父类初始化方法
        super().__init__(api_name="daily", silent=not verbose)
        
        self.target_market_codes = target_market_codes
        self.collection_name = collection_name
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.verbose = verbose
        self.max_workers = max_workers
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.batch_size = batch_size

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

        # 加载接口配置
        self.interface_config = self._load_interface_config()
        
        # 获取Tushare配置
        tushare_config = config_manager.get_tushare_config()
        self.token = tushare_config.get("token")
        self.api_url = tushare_config.get("api_url")
        
        if not self.token or not self.api_url:
            logger.error("未配置Tushare API token或API URL，请检查配置文件")
            raise ValueError("未配置Tushare API token或API URL")
        
        # 获取MongoDB配置
        mongo_config = config_manager.get_mongodb_config()
        self.db_name = mongo_config.get("db_name")
        if not self.db_name:
            logger.warning("未在配置文件中找到MongoDB数据库名称，使用默认名称'tushare_data'")
            self.db_name = "tushare_data"
        
        logger.info(f"MongoDB配置: 数据库名={self.db_name}, 集合名={self.collection_name}")
        logger.info(f"目标市场代码: {', '.join(self.target_market_codes)}")
        
        # 使用平台的MongoDB处理器
        self.mongo_client = mongodb_handler
        
        # 使用平台的端口分配器
        self.port_allocator = port_allocator
        
        # 检查多WAN口支持
        wan_config = config_manager.get_wan_config()
        if wan_config.get("enabled", False):
            wan_count = len(self.port_allocator.get_available_wan_indices())
            logger.info(f"已启用多WAN接口支持，WAN接口数量: {wan_count}")
        else:
            logger.debug("未启用多WAN接口支持")
            
    # 保留其他原有方法，但更新数据库连接和WAN管理相关部分
            
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
            
    def fetch_data(self, **kwargs):
        """
        从数据源获取数据 - 实现BaseFetcher的抽象方法
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            pd.DataFrame: 获取的数据
        """
        # 解析参数
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        ts_code = kwargs.get('ts_code')
        trade_date = kwargs.get('trade_date')
        
        # 根据参数组合选择合适的获取方法
        if trade_date:
            # 按单个交易日获取数据
            return self.fetch_daily_by_date_with_offset(trade_date=trade_date, ts_code=ts_code)
        elif ts_code:
            # 按股票代码获取特定日期范围的数据
            return self.fetch_daily_by_code_with_offset(ts_code=ts_code, start_date=start_date, end_date=end_date)
        elif start_date and end_date:
            # 获取日期范围内所有股票的数据
            target_ts_codes = self.get_target_ts_codes_from_stock_basic()
            if kwargs.get('use_parallel', True) and self.port_allocator:
                return self.fetch_data_parallel(target_ts_codes, start_date, end_date)
            else:
                return self.fetch_data_batch(target_ts_codes, start_date, end_date)
        else:
            logger.error("参数不足，无法获取数据")
            return pd.DataFrame()
    
    def process_data(self, data):
        """
        处理获取的原始数据 - 实现BaseFetcher的抽象方法
        
        Args:
            data: 原始数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        # 过滤目标板块股票
        if data is not None and not data.empty:
            target_ts_codes = self.get_target_ts_codes_from_stock_basic()
            return self.filter_daily_data(data, target_ts_codes)
        return data
    
    def get_collection_name(self):
        """
        获取数据存储的集合名称 - 实现BaseFetcher的抽象方法
        
        Returns:
            str: 集合名称
        """
        return self.collection_name
    
    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """将日线数据保存到MongoDB - 使用平台的MongoDB处理器"""
        if df is None or df.empty:
            logger.warning("没有数据可保存到MongoDB")
            return False
            
        try:
            # 将DataFrame转换为记录列表
            records = df.to_dict('records')
            
            # 获取MongoDB集合
            collection = mongodb_handler.get_collection(self.collection_name)
            
            # 批量保存数据
            result = self._batch_upsert(collection, records, ["ts_code", "trade_date"])
            
            # 记录保存结果
            self.last_operation_stats = result
            
            # 输出统计信息
            logger.success(f"数据处理完成: 新插入 {result['inserted']} 条记录，更新 {result['updated']} 条记录，跳过 {result['skipped']} 条重复记录")
            
            return True
            
        except Exception as e:
            logger.error(f"保存数据到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def _batch_upsert(self, collection, records: List[Dict], unique_keys: List[str]) -> Dict[str, int]:
        """
        批量更新或插入记录，使用唯一键检测记录是否存在
        
        Args:
            collection: MongoDB集合对象
            records: 要保存的记录列表
            unique_keys: 唯一键列表
            
        Returns:
            包含插入、更新和跳过记录数的字典
        """
        if not records:
            return {"inserted": 0, "updated": 0, "skipped": 0}
            
        # 为了提高效率，我们不再逐个检查记录是否存在，而是批量处理
        # 而是用两步策略：先查询哪些记录已存在，然后将记录分为插入和更新两组
        inserted = 0
        updated = 0
        skipped = 0
        
        # 构建所有记录的唯一键查询
        existing_records = set()
        queries = []
        valid_records = []
        
        # 第一步：提取所有有效记录并构建查询条件
        for record in records:
            # 构建查询条件
            query = {}
            key_str = ""
            is_valid = True
            
            for key in unique_keys:
                if key in record and record[key] is not None:
                    query[key] = record[key]
                    key_str += str(record[key]) + "_"
                else:
                    # 缺少唯一键字段，标记为无效
                    is_valid = False
                    break
                    
            if not is_valid or len(query) != len(unique_keys):
                # 跳过无效记录
                skipped += 1
                continue
                
            # 检查是否已处理过相同的记录
            if key_str in existing_records:
                skipped += 1
                continue
                
            # 记录唯一键，准备查询
            existing_records.add(key_str)
            queries.append(query)
            valid_records.append((record, query))
            
        # 第二步：查询哪些记录已经存在
        if not valid_records:
            logger.warning("没有有效记录可以处理")
            return {"inserted": 0, "updated": 0, "skipped": skipped}
            
        # 使用$or查询批量检查记录是否存在
        existing_keys = set()
        try:
            if queries:
                query = {"$or": queries}
                existing_docs = collection.find(query, {"_id": 0, **{k: 1 for k in unique_keys}})
                
                # 记录已存在的记录的唯一键
                for doc in existing_docs:
                    key_values = tuple(doc.get(key) for key in unique_keys)
                    existing_keys.add(key_values)
        except Exception as e:
            logger.error(f"批量查询记录存在性失败: {str(e)}")
            # 如果查询失败，假设所有记录都需要更新
            existing_keys = set()  # 清空集合，后续会执行upsert
            
        # 根据查询结果准备插入和更新操作
        insert_ops = []
        update_ops = []
        
        for record, query in valid_records:
            # 构建唯一键元组
            key_values = tuple(record.get(key) for key in unique_keys)
            
            if key_values in existing_keys:
                # 记录已存在，执行更新
                update_ops.append(
                    pymongo.UpdateOne(
                        query,
                        {"$set": record}
                    )
                )
                updated += 1
            else:
                # 记录不存在，执行插入
                insert_ops.append(pymongo.InsertOne(record))
                inserted += 1
                
        # 分别执行插入和更新操作
        try:
            # 设置合理的WriteConcern参数
            from pymongo import WriteConcern
            temp_collection = collection.with_options(
                write_concern=WriteConcern(w=1, j=False)
            )
            
            # 执行插入操作
            if insert_ops:
                try:
                    insert_result = temp_collection.bulk_write(insert_ops, ordered=False)
                    real_inserted = insert_result.inserted_count
                    if real_inserted != len(insert_ops):
                        logger.warning(f"实际插入数量与预期不一致: 预期={len(insert_ops)}, 实际={real_inserted}")
                        inserted = real_inserted
                except pymongo.errors.BulkWriteError as bwe:
                    # 处理部分失败的插入
                    if hasattr(bwe, 'details'):
                        details = bwe.details
                        if 'nInserted' in details:
                            inserted = details['nInserted']
                        skipped += len(insert_ops) - inserted
                        
                        # 检查是否有重复键错误
                        if 'writeErrors' in details:
                            for error in details['writeErrors']:
                                if error.get('code') == 11000:  # 重复键错误
                                    if self.verbose:
                                        logger.debug(f"插入操作重复键错误: {error.get('errmsg', '')}")
                                        
                    logger.warning(f"插入操作部分失败: {len(bwe.details.get('writeErrors', []))} 错误")
            
            # 执行更新操作
            if update_ops:
                try:
                    update_result = temp_collection.bulk_write(update_ops, ordered=False)
                    real_updated = update_result.modified_count
                    if real_updated != len(update_ops):
                        logger.debug(f"部分记录未被修改，可能数据未变化: 预期={len(update_ops)}, 实际修改={real_updated}")
                except pymongo.errors.BulkWriteError as bwe:
                    # 处理部分失败的更新
                    if hasattr(bwe, 'details'):
                        details = bwe.details
                        if 'nModified' in details:
                            updated = details['nModified']
                        skipped += len(update_ops) - updated
                    logger.warning(f"更新操作部分失败: {len(bwe.details.get('writeErrors', []))} 错误")
                    
        except Exception as e:
            logger.error(f"执行批量操作失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    # 其他方法保持不变，只需要更新与核心平台交互的部分

    def run(self, config=None):
        """
        运行数据获取和保存流程，支持自定义配置 - 实现BaseFetcher的抽象方法
        
        Args:
            config: 配置字典，包含start_date, end_date, full, recent等信息
            
        Returns:
            bool: 是否成功
        """
        # 使用默认配置
        default_config = {
            "start_date": None,
            "end_date": None,
            "full": False,
            "recent": False,
            "ts_code": None,
            "batch_size": 10,  # 每批次处理股票数量
            "use_parallel": True  # 是否使用并行处理
        }
        
        # 合并配置
        if config is None:
            config = {}
        
        effective_config = {**default_config, **config}
        
        # 根据模式运行不同的处理流程
        if effective_config["ts_code"]:
            # 处理单个股票
            return self._run_single_stock(effective_config)
        elif effective_config["full"]:
            # --full 模式: 从1990年至今的所有数据
            return self._run_full_mode(effective_config)
        elif effective_config["recent"]:
            # --recent 模式: 获取最近一周的数据
            return self._run_recent_mode(effective_config)
        elif effective_config["start_date"] and effective_config["end_date"]:
            # --start-date --end-date 模式: 指定日期范围
            return self._run_date_range_mode(effective_config)
        else:
            # 默认模式: 同recent模式
            effective_config["recent"] = True
            return self._run_recent_mode(effective_config)
    
    def _run_single_stock(self, config):
        """处理单个股票数据获取"""
        ts_code = config["ts_code"]
        start_date = config["start_date"]
        end_date = config["end_date"]
        
        # 设置默认日期范围
        if not start_date or not end_date:
            today = datetime.now()
            end_date = today.strftime('%Y%m%d')  # 今天
            start_date = (today - timedelta(days=7)).strftime('%Y%m%d')  # 一周前
            logger.info(f"为指定股票设置默认日期范围: {start_date} 至 {end_date}")
        
        logger.info(f"获取单个股票 {ts_code} 的日线数据，日期范围: {start_date} 至 {end_date}")
        
        # 获取数据
        df = self.fetch_and_save(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        return df is not None and not df.empty
    
    def _run_full_mode(self, config):
        """运行完整历史数据模式"""
        start_date = "19900101"  # 从1990年1月1日开始
        end_date = datetime.now().strftime('%Y%m%d')
        batch_size = config.get("batch_size", 10)
        use_parallel = config.get("use_parallel", True)
        
        logger.info(f"使用full模式：获取 {start_date} 至 {end_date} 期间的全部数据")
        
        # 获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能获取到任何目标板块的股票代码")
            return False
            
        # 判断是否使用并行模式
        if use_parallel and self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            if available_wans:
                logger.info(f"使用并行模式处理 {len(target_ts_codes)} 个股票")
                result = self.fetch_data_parallel(target_ts_codes, start_date, end_date, batch_size)
                return result is not None
        
        # 串行模式
        logger.info(f"使用串行模式处理 {len(target_ts_codes)} 个股票")
        result = self.fetch_data_batch(target_ts_codes, start_date, end_date, batch_size)
        return result is not None
    
    def _run_recent_mode(self, config):
        """运行最近数据更新模式"""
        today = datetime.now()
        end_date = today.strftime('%Y%m%d')
        start_date = (today - timedelta(days=7)).strftime('%Y%m%d')
        use_parallel = config.get("use_parallel", True)
        
        logger.info(f"使用recent模式：获取 {start_date} 至 {end_date} 期间的交易日数据")
        
        # 从交易日历获取交易日
        trade_dates = self.get_trade_calendar(start_date, end_date)
        if not trade_dates:
            logger.warning(f"日期范围 {start_date} 至 {end_date} 内无交易日")
            return False
        
        logger.info(f"从交易日历获取到 {len(trade_dates)} 个交易日，将逐日获取全市场数据")
        
        # 判断是否使用并行模式
        if use_parallel and self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            if available_wans:
                logger.info(f"使用并行模式处理 {len(trade_dates)} 个交易日")
                return self._fetch_parallel_by_dates_direct(trade_dates)
        
        # 串行模式
        logger.info(f"使用串行模式处理 {len(trade_dates)} 个交易日")
        return self._fetch_sequential_by_dates_direct(trade_dates)
    
    def _run_date_range_mode(self, config):
        """运行指定日期范围模式"""
        start_date = config["start_date"]
        end_date = config["end_date"]
        batch_size = config.get("batch_size", 10)
        use_parallel = config.get("use_parallel", True)
        
        logger.info(f"使用日期范围模式：获取 {start_date} 至 {end_date} 期间的数据")
        
        # 获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能获取到任何目标板块的股票代码")
            return False
        
        # 判断是否使用并行模式
        if use_parallel and self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            if available_wans:
                logger.info(f"使用并行模式处理 {len(target_ts_codes)} 个股票代码")
                result = self.fetch_data_parallel(target_ts_codes, start_date, end_date, batch_size)
                return result is not None
        
        # 串行模式
        logger.info(f"使用串行模式处理 {len(target_ts_codes)} 个股票代码")
        result = self.fetch_data_batch(target_ts_codes, start_date, end_date, batch_size)
        return result is not None

    def fetch_stock_basic(self) -> bool:
        """
        获取并保存股票基本信息
        
        Returns:
            bool: 是否成功获取并保存
        """
        try:
            logger.info("获取股票基本信息...")
            
            # 准备API参数
            api_name = "stock_basic"
            params = {
                "exchange": "",
                "list_status": "L"  # 上市状态：L上市 D退市 P暂停上市
            }
            # 只请求API支持的字段
            fields = ["ts_code", "symbol", "name", "list_date"]
            
            # 使用父类的方法获取数据
            df = None
            
            # 尝试使用tushare_client直接获取
            try:
                # 创建TushareClientWAN实例
                client = TushareClientWAN(token=self.token, timeout=60, api_url=self.api_url)
                df = client.get_data(api_name=api_name, params=params, fields=fields)
            except Exception as e:
                logger.error(f"使用TushareClientWAN获取股票基本信息失败: {str(e)}")
            
            # 如果上面方法失败，尝试使用WAN接口
            if df is None or df.empty:
                logger.info("尝试使用WAN接口获取股票基本信息...")
                
                # 尝试使用WAN接口
                if self.port_allocator:
                    available_wans = self.port_allocator.get_available_wan_indices()
                    if available_wans:
                        wan_idx = available_wans[0]
                        wan_info = self._get_wan_socket(wan_idx)
                        
                        if wan_info:
                            wan_idx, port = wan_info
                            try:
                                client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
                                client.set_local_address('0.0.0.0', port)
                                df = client.get_data(api_name=api_name, params=params, fields=fields)
                            finally:
                                # 释放端口
                                self.port_allocator.release_port(wan_idx, port)
            
            # 检查结果
            if df is None or df.empty:
                logger.error("获取股票基本信息失败，API返回数据为空")
                return False
                
            # 输出获取结果
            logger.success(f"成功获取 {len(df)} 条股票基本信息")
            
            # 保存到MongoDB
            collection = mongodb_handler.get_collection("stock_basic")
            
            # 确保索引存在
            collection.create_index("ts_code", unique=True, background=True)
            collection.create_index("symbol", background=True)
            
            # 转换为记录列表
            records = df.to_dict('records')
            
            # 批量更新插入
            insert_count = 0
            update_count = 0
            
            for record in records:
                try:
                    # 使用ts_code作为查询条件进行upsert
                    result = collection.update_one(
                        {"ts_code": record["ts_code"]},
                        {"$set": record},
                        upsert=True
                    )
                    
                    if result.upserted_id:
                        insert_count += 1
                    elif result.modified_count > 0:
                        update_count += 1
                except Exception as e:
                    logger.error(f"保存记录 {record.get('ts_code')} 失败: {str(e)}")
            
            logger.success(f"股票基本信息保存完成：新增 {insert_count} 条，更新 {update_count} 条")
            return True
            
        except Exception as e:
            logger.error(f"获取股票基本信息失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def get_target_ts_codes_from_stock_basic(self) -> Set[str]:
        """
        从stock_basic集合中获取目标板块的股票代码
        
        Returns:
            目标板块股票代码集合
        """
        try:
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return set()
                
            # 查询stock_basic集合中符合条件的股票代码
            logger.info(f"从stock_basic集合查询目标板块 {self.target_market_codes} 的股票代码")
            
            # 构建查询条件：symbol前两位在target_market_codes中
            query_conditions = []
            for market_code in self.target_market_codes:
                # 使用正则表达式匹配symbol前两位
                query_conditions.append({"symbol": {"$regex": f"^{market_code}"}})
                
            # 使用$or操作符组合多个条件
            query = {"$or": query_conditions} if query_conditions else {}
            
            # 只查询ts_code字段
            collection = mongodb_handler.get_collection("stock_basic")
            result = collection.find(query, {"ts_code": 1, "_id": 0})
            
            # 提取ts_code集合
            ts_codes = set()
            for doc in result:
                if "ts_code" in doc:
                    ts_codes.add(doc["ts_code"])
            
            # 如果没有获取到数据，先尝试获取股票基本信息
            if not ts_codes:
                logger.warning("未从stock_basic集合获取到任何股票代码，尝试更新股票基本信息...")
                if self.fetch_stock_basic():
                    # 重新查询
                    result = collection.find(query, {"ts_code": 1, "_id": 0})
            for doc in result:
                if "ts_code" in doc:
                    ts_codes.add(doc["ts_code"])
            
            logger.success(f"从stock_basic集合获取到 {len(ts_codes)} 个目标股票代码")
            
            # 如果仍然没有获取到数据，提供更详细的警告
            if not ts_codes:
                logger.warning("即使更新了股票基本信息，仍未获取到任何目标板块的股票代码")
                logger.warning("可能的原因：1. API接口不可用; 2. 目标市场代码不正确; 3. 数据库查询条件不匹配")
            
            # 输出详细日志
            if self.verbose and ts_codes:
                sample_codes = list(ts_codes)[:5] if ts_codes else []
                logger.debug(f"样例股票代码: {sample_codes}")
                
            return ts_codes
            
        except Exception as e:
            logger.error(f"查询stock_basic集合失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return set()

    def _ensure_indexes(self, collection) -> bool:
        """
        确保必要的索引存在
        
        Args:
            collection: MongoDB集合对象
            
        Returns:
            是否需要创建索引
        """
        try:
            # 获取现有索引
            existing_indexes = collection.index_information()
            logger.debug(f"现有索引信息: {existing_indexes}")
            
            # 检查复合唯一索引 (ts_code, trade_date)
            index_name = "ts_code_1_trade_date_1"
            index_created = False
            
            # 检查索引是否存在并且结构正确
            if index_name in existing_indexes:
                # 验证索引的键和属性
                existing_index = existing_indexes[index_name]
                expected_keys = [("ts_code", 1), ("trade_date", 1)]
                
                # 确保是有序的正确键和唯一约束
                keys_match = all(key in expected_keys for key in existing_index['key']) and len(existing_index['key']) == len(expected_keys)
                is_unique = existing_index.get('unique', False)
                
                if keys_match and is_unique:
                    logger.debug(f"复合唯一索引 (ts_code, trade_date) 已存在且结构正确，跳过创建")
                else:
                    # 索引存在但结构不正确，删除并重建
                    logger.info(f"复合唯一索引 (ts_code, trade_date) 存在但结构不正确，删除并重建索引")
                    try:
                        collection.drop_index(index_name)
                        logger.debug(f"成功删除现有索引: {index_name}")
                    except Exception as e:
                        logger.error(f"删除索引时出错: {str(e)}")
                    
                    # 创建正确的索引
                    collection.create_index(
                        [("ts_code", 1), ("trade_date", 1)], 
                        unique=True, 
                        background=True
                    )
                    logger.success(f"已重建复合唯一索引 (ts_code, trade_date)")
                    index_created = True
            else:
                # 索引不存在，创建它
                logger.info(f"正在为集合 {collection.name} 创建复合唯一索引 (ts_code, trade_date)...")
                collection.create_index(
                    [("ts_code", 1), ("trade_date", 1)], 
                    unique=True, 
                    background=True
                )
                logger.success(f"已成功创建复合唯一索引 (ts_code, trade_date)")
                index_created = True
            
            # 检查单字段索引
            for field in ["ts_code", "trade_date"]:
                index_field_name = f"{field}_1"
                if index_field_name not in existing_indexes:
                    logger.info(f"正在为字段 {field} 创建索引...")
                    collection.create_index(field)
                    logger.success(f"已为字段 {field} 创建索引")
                    index_created = True
                else:
                    logger.debug(f"字段 {field} 的索引已存在，跳过创建")
            
            # 确保在创建索引后等待一小段时间，让MongoDB完成索引构建
            if index_created:
                logger.info("索引已创建或修改，等待MongoDB完成索引构建...")
                time.sleep(1.0)  # 等待1秒，让MongoDB完成索引构建
            
            return True
                    
        except Exception as e:
            logger.error(f"创建索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def get_trade_calendar(self, start_date: str, end_date: str) -> List[str]:
        """
        从mongodb中获取指定日期范围内的交易日历
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            交易日期列表，格式为YYYYMMDD
        """
        try:
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return []
            
            logger.info(f"尝试获取日期范围 {start_date} 至 {end_date} 的交易日")
            
            # 从trade_cal集合获取交易日历，使用正确字段名trade_date
            try:
                collection = mongodb_handler.get_collection("trade_cal")
                
                # 查询交易日期
                query = {
                    "trade_date": {"$gte": start_date, "$lte": end_date}
                }
                
                # 只查询日期字段
                result = collection.find(query, {"trade_date": 1, "_id": 0}).sort("trade_date", 1)
                
                # 提取日期列表
                trade_dates = [doc["trade_date"] for doc in result]
                
                logger.info(f"从trade_cal集合获取到 {len(trade_dates)} 个交易日")
                
                if not trade_dates:
                    logger.warning(f"未从trade_cal集合获取到日期范围 {start_date} 至 {end_date} 内的交易日，将使用日期范围内的所有日期")
                    # 生成日期范围内的所有日期
                    start_date_obj = datetime.strptime(start_date, '%Y%m%d')
                    end_date_obj = datetime.strptime(end_date, '%Y%m%d')
                    
                    trade_dates = []
                    current_date = start_date_obj
                    while current_date <= end_date_obj:
                        trade_dates.append(current_date.strftime('%Y%m%d'))
                        current_date += timedelta(days=1)
                    
                    logger.info(f"生成日期范围内的 {len(trade_dates)} 个日期")
                
                return trade_dates
                
            except Exception as e:
                logger.error(f"获取交易日历失败: {str(e)}")
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
                
                # 出错时生成日期范围内的所有日期作为备选
                start_date_obj = datetime.strptime(start_date, '%Y%m%d')
                end_date_obj = datetime.strptime(end_date, '%Y%m%d')
                
                trade_dates = []
                current_date = start_date_obj
                while current_date <= end_date_obj:
                    trade_dates.append(current_date.strftime('%Y%m%d'))
                    current_date += timedelta(days=1)
                
                logger.info(f"生成日期范围内的 {len(trade_dates)} 个日期作为备选")
                return trade_dates
                
        except Exception as e:
            logger.error(f"获取交易日历失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 兜底方案：生成日期范围内的所有日期
            start_date_obj = datetime.strptime(start_date, '%Y%m%d')
            end_date_obj = datetime.strptime(end_date, '%Y%m%d')
            
            trade_dates = []
            current_date = start_date_obj
            while current_date <= end_date_obj:
                trade_dates.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=1)
            
            logger.info(f"兜底方案：生成日期范围内的 {len(trade_dates)} 个日期")
            return trade_dates

    def _fetch_parallel_by_dates_direct(self, date_list: List[str]) -> bool:
        """
        使用多WAN口并行获取多个日期的数据，直接保存目标板块数据
        
        Args:
            date_list: 日期列表
            
        Returns:
            是否成功
        """
        import threading
        import queue
        
        total_days = len(date_list)
        # 设置最大并发数
        available_wans = self.port_allocator.get_available_wan_indices()
        max_workers = min(len(available_wans), total_days, 4)  # 最多4个并发，避免过多资源消耗
        result_queue = queue.Queue()
        active_threads = []
        
        # 线程锁用于日志和进度更新
        log_lock = threading.Lock()
        
        total_records = 0
        processed_days = 0
        success_days = 0
        
        # 先确保索引
        self._ensure_collection_indexes()
        
        # 线程函数 - 处理单个日期的数据
        def process_date(date_str, wan_idx):
            try:
                with log_lock:
                    logger.debug(f"WAN {wan_idx} 开始处理日期 {date_str}")
                
                # 获取WAN端口
                wan_info = self._get_wan_socket(wan_idx)
                if not wan_info:
                    with log_lock:
                        logger.warning(f"无法为WAN {wan_idx} 获取端口，使用默认网络接口")
                    # 注意：这里不使用ts_code参数，按日期获取全市场数据
                    df = self.fetch_daily_by_date_with_offset(trade_date=date_str)
                else:
                    # 使用WAN口获取数据
                    wan_idx, port = wan_info
                    try:
                        # 使用WAN接口获取当日数据，不使用ts_code参数
                        df = self.fetch_daily_by_date_with_wan(trade_date=date_str, wan_info=wan_info)
                    finally:
                        # 确保释放WAN端口
                        if wan_info:
                            # 添加短暂延迟，确保端口完全释放
                            time.sleep(0.5)
                            self.port_allocator.release_port(wan_idx, port)
                
                success = False
                records_count = 0
                inserted_count = 0
                updated_count = 0
                skipped_count = 0
                
                if df is not None and not df.empty:
                    # 只保留目标板块的股票数据
                    df_filtered = df[df['ts_code'].str.slice(0, 2).isin(self.target_market_codes)]
                    
                    # 保存到MongoDB
                    if df_filtered is not None and not df_filtered.empty:
                        save_success = self.save_to_mongodb(df_filtered)
                        records_count = len(df_filtered)
                        
                        # 获取详细统计数据 - 最近一次操作的结果存储在类的属性中
                        if hasattr(self, 'last_operation_stats'):
                            inserted_count = self.last_operation_stats.get('inserted', 0)
                            updated_count = self.last_operation_stats.get('updated', 0)
                            skipped_count = self.last_operation_stats.get('skipped', 0)
                        
                        success = save_success
                
                # 放入结果队列 - 增加更多统计信息
                result_queue.put((date_str, success, records_count, inserted_count, updated_count, skipped_count))
                
                with log_lock:
                    if success:
                        logger.success(f"WAN {wan_idx} 成功处理日期 {date_str} 的数据，共 {records_count} 条记录，新增 {inserted_count}，更新 {updated_count}，跳过 {skipped_count}")
                    else:
                        logger.warning(f"WAN {wan_idx} 处理日期 {date_str} 失败或无数据")
            
            except Exception as e:
                with log_lock:
                    logger.error(f"WAN {wan_idx} 处理日期 {date_str} 出错: {str(e)}")
                    import traceback
                    logger.debug(f"详细错误信息: {traceback.format_exc()}")
                result_queue.put((date_str, False, 0, 0, 0, 0))
                
                # 确保释放WAN端口
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    # 添加短暂延迟，确保端口完全释放
                    time.sleep(0.5)
                    self.port_allocator.release_port(wan_idx, port)
        
        # 循环处理所有日期，控制最大线程数
        start_time = time.time()
        
        # 用于统计的变量
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        
        for i, date_str in enumerate(date_list):
            # 选择WAN接口 - 轮询方式
            wan_idx = available_wans[i % len(available_wans)]
            
            # 创建并启动线程
            thread = threading.Thread(
                target=process_date,
                args=(date_str, wan_idx)
            )
            thread.start()
            active_threads.append(thread)
            
            # 控制最大并发数
            if len(active_threads) >= max_workers:
                # 等待一个线程完成
                while result_queue.empty():
                    time.sleep(0.5)
                
                # 处理结果
                date_str, success, records_count, inserted, updated, skipped = result_queue.get()
                processed_days += 1
                if success:
                    success_days += 1
                    total_records += records_count
                    total_inserted += inserted
                    total_updated += updated
                    total_skipped += skipped
                
                # 更新进度
                elapsed = time.time() - start_time
                avg_time_per_day = elapsed / processed_days if processed_days > 0 else 0
                remaining = (total_days - processed_days) * avg_time_per_day
                progress = processed_days / total_days * 100
                logger.info(f"日期进度: {processed_days}/{total_days} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
                
                # 移除已完成的线程
                active_threads = [t for t in active_threads if t.is_alive()]
                
                # 短暂休眠，避免过于频繁的请求
                time.sleep(0.5)
        
        # 等待所有剩余线程完成
        for thread in active_threads:
            thread.join()
        
        # 处理剩余结果
        while not result_queue.empty():
            date_str, success, records_count, inserted, updated, skipped = result_queue.get()
            processed_days += 1
            if success:
                success_days += 1
                total_records += records_count
                total_inserted += inserted
                total_updated += updated
                total_skipped += skipped
        
        # 处理完成
        elapsed_total = time.time() - start_time
        
        # 输出详细统计信息
        logger.success(f"按日期并行获取数据完成，成功处理 {success_days}/{total_days} 天")
        logger.info(f"数据统计: 总记录数 {total_records}，新增 {total_inserted}，更新 {total_updated}，跳过 {total_skipped}，耗时 {elapsed_total:.1f}s")
        
        # 即使只有一天成功，也认为处理成功
        return success_days > 0

    def _fetch_sequential_by_dates_direct(self, date_list: List[str]) -> bool:
        """
        按顺序获取多个日期的数据，直接保存目标板块数据
        
        Args:
            date_list: 日期列表
            
        Returns:
            是否成功
        """
        total_days = len(date_list)
        total_records = 0
        processed_days = 0
        success_days = 0
        
        # 用于统计的变量
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        
        # 先确保索引
        self._ensure_collection_indexes()
        
        start_time = time.time()
        
        # 逐日获取数据
        for date_str in date_list:
            logger.info(f"正在获取日期 {date_str} 的日线数据...")
            
            # 获取当日所有股票数据，使用支持offset的方法，不传递ts_code参数
            df = self.fetch_daily_by_date_with_offset(trade_date=date_str)
            
            if df is not None and not df.empty:
                # 只保留目标板块的股票数据
                df_filtered = df[df['ts_code'].str.slice(0, 2).isin(self.target_market_codes)]
                
                # 保存到MongoDB
                if df_filtered is not None and not df_filtered.empty:
                    success = self.save_to_mongodb(df_filtered)
                    
                    # 获取详细统计数据
                    inserted = 0
                    updated = 0
                    skipped = 0
                    if hasattr(self, 'last_operation_stats'):
                        inserted = self.last_operation_stats.get('inserted', 0)
                        updated = self.last_operation_stats.get('updated', 0)
                        skipped = self.last_operation_stats.get('skipped', 0)
                    
                    if success:
                        success_days += 1
                        total_records += len(df_filtered)
                        total_inserted += inserted
                        total_updated += updated
                        total_skipped += skipped
                        logger.success(f"成功保存 {date_str} 的日线数据，{len(df_filtered)} 条记录，新增 {inserted}，更新 {updated}，跳过 {skipped}")
                    else:
                        logger.warning(f"保存 {date_str} 的日线数据失败")
                else:
                    logger.warning(f"日期 {date_str} 过滤后无目标板块股票数据")
            else:
                logger.warning(f"日期 {date_str} 未获取到数据")
            
            # 更新进度
            processed_days += 1
            progress = processed_days / total_days * 100
            elapsed = time.time() - start_time
            avg_time_per_day = elapsed / processed_days if processed_days > 0 else 0
            remaining = (total_days - processed_days) * avg_time_per_day
            logger.info(f"日期进度: {processed_days}/{total_days} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
            
            # 进入下一天前短暂休眠
            time.sleep(1)
        
        # 处理完成
        elapsed_total = time.time() - start_time
        
        # 输出详细统计信息
        logger.success(f"按日期顺序获取数据完成，成功处理 {success_days}/{total_days} 天")
        logger.info(f"数据统计: 总记录数 {total_records}，新增 {total_inserted}，更新 {total_updated}，跳过 {total_skipped}，耗时 {elapsed_total:.1f}s")
        
        # 即使只有一天成功，也认为处理成功
        return success_days > 0
        
    def _ensure_collection_indexes(self) -> bool:
        """
        根据接口配置确保集合索引存在
        
        Returns:
            bool: 是否成功创建或验证索引
        """
        try:
            # 获取MongoDB集合
            collection = mongodb_handler.get_collection(self.collection_name)
            
            # 获取现有索引
            existing_indexes = collection.index_information()
            logger.debug(f"现有索引信息: {existing_indexes}")
            
            # 从接口配置获取索引字段
            index_fields = self.interface_config.get("index_fields", ["ts_code", "trade_date"])
            logger.info(f"从接口配置获取索引字段: {index_fields}")
            
            # 检查复合唯一索引
            index_name = "_".join([f"{field}_1" for field in index_fields])
            index_created = False
            
            # 检查索引是否存在并且结构正确
            if index_name in existing_indexes:
                # 验证索引的键和属性
                existing_index = existing_indexes[index_name]
                expected_keys = [(field, 1) for field in index_fields]
                
                # 确保是有序的正确键和唯一约束
                keys_match = all(key in expected_keys for key in existing_index['key']) and len(existing_index['key']) == len(expected_keys)
                is_unique = existing_index.get('unique', False)
                
                if keys_match and is_unique:
                    logger.debug(f"复合唯一索引 {index_fields} 已存在且结构正确，跳过创建")
                else:
                    # 索引存在但结构不正确，删除并重建
                    logger.info(f"复合唯一索引 {index_fields} 存在但结构不正确，删除并重建索引")
                    try:
                        collection.drop_index(index_name)
                        logger.debug(f"成功删除现有索引: {index_name}")
                    except Exception as e:
                        logger.error(f"删除索引时出错: {str(e)}")
                    
                    # 创建正确的索引
                    collection.create_index(
                        [(field, 1) for field in index_fields], 
                        unique=True, 
                        background=True
                    )
                    logger.success(f"已重建复合唯一索引 {index_fields}")
                    index_created = True
            else:
                # 索引不存在，创建它
                logger.info(f"正在为集合 {collection.name} 创建复合唯一索引 {index_fields}...")
                collection.create_index(
                    [(field, 1) for field in index_fields], 
                    unique=True, 
                    background=True
                )
                logger.success(f"已成功创建复合唯一索引 {index_fields}")
                index_created = True
            
            # 检查单字段索引
            for field in index_fields:
                index_field_name = f"{field}_1"
                if index_field_name not in existing_indexes:
                    logger.info(f"正在为字段 {field} 创建索引...")
                    collection.create_index(field)
                    logger.success(f"已为字段 {field} 创建索引")
                    index_created = True
                else:
                    logger.debug(f"字段 {field} 的索引已存在，跳过创建")
            
            # 确保在创建索引后等待一小段时间，让MongoDB完成索引构建
            if index_created:
                logger.info("索引已创建或修改，等待MongoDB完成索引构建...")
                time.sleep(1.0)  # 等待1秒，让MongoDB完成索引构建
            
            return True
                    
        except Exception as e:
            logger.error(f"创建索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def _get_wan_socket(self, wan_idx: int = None) -> Optional[Tuple[int, int]]:
        """
        获取WAN接口和端口
        
        Args:
            wan_idx: 指定WAN接口索引，如果为None则自动选择
            
        Returns:
            (wan_idx, port) 元组，或者None表示失败
        """
        if not self.port_allocator:
            return None
            
        try:
            # 获取可用的WAN接口索引
            available_indices = self.port_allocator.get_available_wan_indices()
            if not available_indices:
                logger.warning("没有可用的WAN接口")
                return None
            
            # 如果指定了WAN索引，检查是否可用
            if wan_idx is not None:
                if wan_idx not in available_indices:
                    logger.warning(f"指定的WAN {wan_idx} 不可用，尝试自动选择")
                    wan_idx = None
                
            # 如果未指定或指定的不可用，自动选择一个WAN接口
            if wan_idx is None:
                # 轮询选择一个WAN接口
                wan_idx = available_indices[0]  # 简单起见，选择第一个
            
            # 分配端口
            retry_count = 10
            port = None
            
            while retry_count > 0 and port is None:
                port = self.port_allocator.allocate_port(wan_idx)
                if port:
                    logger.debug(f"使用WAN接口 {wan_idx}，本地端口 {port}")
                    # 添加短暂延迟，确保端口完全释放
                    time.sleep(0.5)  # 增加到500毫秒的延迟，有助于避免端口重用问题
                    return (wan_idx, port)
                else:
                    logger.warning(f"WAN {wan_idx} 没有可用端口，重试 {retry_count}")
                    retry_count -= 1
                    time.sleep(0.5)  # 等待0.5秒再重试
            
            if port is None:
                logger.warning(f"WAN {wan_idx} 经过多次尝试仍没有可用端口")
                return None
            
            return (wan_idx, port)
            
        except Exception as e:
            logger.error(f"获取WAN接口失败: {str(e)}")
            return None
    
    def filter_daily_data(self, df: pd.DataFrame, target_ts_codes: Set[str]) -> pd.DataFrame:
        """
        根据目标股票代码集合过滤日线数据
        
        Args:
            df: 日线数据
            target_ts_codes: 目标股票代码集合
            
        Returns:
            过滤后的数据
        """
        if df is None or df.empty:
            logger.warning("没有日线数据可过滤")
            return pd.DataFrame()
        
        logger.info(f"过滤前日线数据数量: {len(df)}")
        
        # 确保ts_code列存在
        if 'ts_code' not in df.columns:
            logger.error("数据中没有ts_code列，无法按股票代码过滤")
            return df
        
        # 过滤数据
        df_filtered = df[df['ts_code'].isin(target_ts_codes)].copy()
        
        # 输出过滤统计信息
        logger.info(f"过滤后日线数据数量: {len(df_filtered)}")
        
        # 详细统计信息
        if self.verbose:
            # 统计各市场的股票数量
            if not df_filtered.empty and 'ts_code' in df_filtered.columns:
                # 从ts_code提取市场代码
                df_filtered['market_code'] = df_filtered['ts_code'].str[:6].str[:2]
                market_stats = df_filtered['market_code'].value_counts().to_dict()
                
                logger.debug("过滤后各市场代码分布:")
                for code, count in sorted(market_stats.items()):
                    logger.debug(f"  {code}: {count} 记录")
        
        return df_filtered
        
    def fetch_daily_by_date_with_offset(self, trade_date: str, ts_code: str = None, max_count: int = 9000) -> pd.DataFrame:
        """
        按日期获取日线行情数据，支持offset处理超过API限制的数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            max_count: 每次请求的最大记录数
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            # 使用父类的通用方法进行API调用
            df = self.fetch_data(**params)
            
            if df is None or df.empty:
                logger.warning(f"日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                return pd.DataFrame()
            
            logger.info(f"成功获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {len(df)} 条记录"))
            return df
            
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的日线数据失败: {str(e)}")
            return pd.DataFrame()
            
    def fetch_daily_by_date_with_wan(self, trade_date: str, ts_code: str = None, wan_info: Tuple[int, int] = None, max_count: int = 9000) -> pd.DataFrame:
        """
        使用WAN接口按日期获取日线行情数据，支持offset处理超过API限制的数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            wan_info: WAN接口和端口信息(wan_idx, port)
            max_count: 每次请求的最大记录数
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            # 准备请求参数
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"使用WAN接口获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            # 准备API参数
            api_name = "daily"
            fields = self.interface_config.get("available_fields", [])
            
            # 创建WAN专用客户端
            if not wan_info:
                logger.warning("未提供WAN接口信息，使用普通客户端获取数据")
                return self.fetch_daily_by_date_with_offset(trade_date=trade_date, ts_code=ts_code, max_count=max_count)
            
            wan_idx, port = wan_info
            client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
            try:
                client.set_local_address('0.0.0.0', port)
                
                # 使用偏移量处理数据超限
                all_data = []
                offset = 0
                
                while True:
                    # 复制参数并添加分页参数
                    current_params = params.copy()
                    current_params["offset"] = offset
                    current_params["limit"] = max_count
                    
                    # 获取数据
                    start_time = time.time()
                    df = client.get_data(api_name=api_name, params=current_params, fields=fields)
                    elapsed = time.time() - start_time
                    
                    if df is None or df.empty:
                        if offset == 0:
                            logger.warning(f"WAN {wan_idx} 获取日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                            return pd.DataFrame()
                        else:
                            # 已经获取了一部分数据，当前批次为空表示数据已获取完毕
                            break
                    
                    # 添加到结果列表
                    all_data.append(df)
                    records_count = len(df)
                    logger.debug(f"WAN {wan_idx} 获取到 {records_count} 条记录，偏移量: {offset}, 耗时: {elapsed:.2f}s")
                    
                    # 如果返回的数据量等于最大请求数量，可能还有更多数据
                    if records_count == max_count:
                        logger.info(f"WAN {wan_idx} 返回数据量达到单次请求上限 {max_count}，将继续获取下一批数据")
                    # 如果返回的数据量小于请求的数量，说明已经没有更多数据
                    if records_count < max_count:
                        break
                        
                    # 设置下一批次的偏移量
                    offset += max_count
                    
                    # 短暂休眠，避免过于频繁的请求
                    time.sleep(0.5)
                
                # 合并所有数据
                if not all_data:
                    return pd.DataFrame()
                    
                result_df = pd.concat(all_data, ignore_index=True)
                total_records = len(result_df)
                
                # 只有当总记录数超过9500才提示可能数据不完整
                if total_records > 9500 and len(all_data) == 1:
                    logger.warning(f"WAN {wan_idx} 股票数据总量 {total_records} 接近API限制(10000)，可能数据不完整，建议按时间段分割获取")
                
                logger.info(f"WAN {wan_idx} 成功获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {total_records} 条记录"))
                return result_df
            finally:
                # 重置客户端设置，确保资源释放
                if hasattr(client, 'reset_local_address'):
                    client.reset_local_address()
                
        except Exception as e:
            logger.error(f"WAN接口获取日期 {trade_date} 的日线数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return pd.DataFrame()

def main():
    """主函数"""
    import argparse
    from datetime import datetime, timedelta
    import logging
    import traceback
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="日线行情数据获取工具")
    parser.add_argument("--verbose", action="store_true", help="输出详细日志")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据模式（API不可用时）")
    parser.add_argument("--start-date", type=str, help="开始日期，格式为YYYYMMDD，如20100101")
    parser.add_argument("--end-date", type=str, help="结束日期，格式为YYYYMMDD，如20201231")
    parser.add_argument("--recent", action="store_true", help="按trade_date分批获取最近一周的数据，通过交易日历过滤")
    parser.add_argument("--full", action="store_true", help="获取从1990年1月1日至今的完整历史数据，按ts_code列表分批获取")
    parser.add_argument("--ts-code", type=str, help="指定股票代码，例如600000.SH")
    parser.add_argument("--batch-size", type=int, default=1, help="每批次处理的股票数量，默认1")
    parser.add_argument("--market-codes", type=str, default="00,30,60,68", help="目标市场代码，用逗号分隔，默认为00,30,60,68")
    parser.add_argument("--no-parallel", dest="use_parallel", action="store_false", help="不使用并行处理")
    parser.set_defaults(use_parallel=True)
    args = parser.parse_args()
    
    # 初始化日志
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    log = logging.getLogger("daily_fetcher")
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    try:
        # 确保配置已加载
        if not hasattr(config_manager, 'config') or not config_manager.config:
            log.info("正在加载平台配置...")
            config_manager.load_config()
        
        # 确保MongoDB已连接
        if not mongodb_handler.is_connected():
            log.info("正在连接MongoDB...")
            mongodb_handler.connect()
        
        # 确保网络管理器已初始化
        if network_manager and hasattr(network_manager, 'refresh_interfaces'):
            log.info("正在刷新网络接口状态...")
            network_manager.refresh_interfaces()
        
        # 创建获取器
        fetcher = DailyFetcher(
            verbose=args.verbose, 
            batch_size=args.batch_size,
            target_market_codes=target_market_codes
        )
        
        # 构建运行配置字典
        run_config = {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "full": args.full,
            "recent": args.recent,
            "ts_code": args.ts_code,
            "batch_size": args.batch_size,
            "use_parallel": args.use_parallel,
            "mock": args.mock
        }
        
        # 运行获取器
        success = fetcher.run(run_config)
        
        if success:
            log.info("日线数据获取和保存成功")
            return 0
        else:
            log.error("日线数据获取或保存失败")
            return 1
            
    except Exception as e:
        log.error(f"程序运行出错: {str(e)}")
        log.error(f"详细错误信息: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
