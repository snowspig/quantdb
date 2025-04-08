#!/usr/bin/env python
"""
Trade Calendar Fetcher - 获取交易日历数据并保存到MongoDB

该脚本用于从湘财Tushare获取交易日历数据，并保存到MongoDB数据库中的trade_cal集合
默认模式下抓取最近一年的交易日历数据

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=26

使用方法：
    python trade_cal_fetcher.py                   # 使用湘财真实API数据，简洁日志模式，获取近期数据
    python trade_cal_fetcher.py --verbose         # 使用湘财真实API数据，详细日志模式
    python trade_cal_fetcher.py --start-date 20200101 --end-date 20231231  # 指定日期范围
"""
import os
import sys
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from loguru import logger
import pymongo
import random

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core import config_manager, mongodb_handler
from core.wan_manager import port_allocator
from core.tushare_client_wan import TushareClientWAN

class TradeCalFetcher:
    """
    交易日历数据获取器
    
    该类用于从Tushare获取交易日历数据并保存到MongoDB数据库
    默认模式下使用多WAN口模块抓取数据，并获取近期交易日历数据
    """
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "trade_cal.json",
        db_name: str = None,
        collection_name: str = "trade_cal",
        start_date: str = None,
        end_date: str = None,
        exchange: str = "SSE",  # 默认上交所
        verbose: bool = False
    ):
        """
        初始化交易日历数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            start_date: 开始日期（格式：YYYYMMDD，默认为当前日期前一年）
            end_date: 结束日期（格式：YYYYMMDD，默认为当前日期）
            exchange: 交易所代码（SSE：上交所，SZSE：深交所，默认SSE）
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.collection_name = collection_name
        self.exchange = exchange
        self.verbose = verbose
        
        # 设置默认日期范围（如果未提供）
        if not start_date or not end_date:
            today = datetime.now()
            if not end_date:
                self.end_date = today.strftime("%Y%m%d")
            else:
                self.end_date = end_date
                
            if not start_date:
                # 默认获取最近一年的数据
                one_year_ago = today - timedelta(days=365)
                self.start_date = one_year_ago.strftime("%Y%m%d")
            else:
                self.start_date = start_date
        else:
            self.start_date = start_date
            self.end_date = end_date
            
        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
        
        # 加载配置
        self.config = config_manager.get_all_config()
        self.interface_config = self._load_interface_config()
        
        # 获取token和api_url - 从配置文件读取
        tushare_config = self.config.get("tushare", {})
        self.token = tushare_config.get("token", "")
        self.api_url = tushare_config.get("api_url", "")
        
        # 从配置中读取db_name
        mongodb_config = self.config.get("mongodb", {})
        config_db_name = mongodb_config.get("db_name", "tushare_data")
        # 如果未传入db_name或传入为None，则使用配置文件中的值
        self.db_name = db_name if db_name is not None else config_db_name
        logger.debug(f"使用数据库名称: {self.db_name}")
        
        if not self.token:
            logger.error("未配置Tushare API Token")
            sys.exit(1)
            
        # 记录API配置信息
        mask_token = self.token[:4] + '*' * (len(self.token) - 8) + self.token[-4:] if len(self.token) > 8 else '***'
        logger.debug(f"获取到的API token长度: {len(self.token)}")
        logger.debug(f"获取到的API URL: {self.api_url}")
        
        # 初始化Tushare客户端
        self.client = TushareClientWAN(token=self.token, api_url=self.api_url)
        
        # 确保MongoDB连接初始化
        if not mongodb_handler.is_connected():
            logger.info("初始化MongoDB连接...")
            if mongodb_handler.connect():
                logger.info("MongoDB连接成功")
            else:
                logger.error("连接MongoDB失败")
                sys.exit(1)
        else:
            logger.info("MongoDB已连接")
        
        # 验证MongoDB连接和写入权限
        self._verify_mongodb_connection()
            
        # 记录可用字段和索引字段
        self.available_fields = self.interface_config.get("available_fields", [])
        self.index_fields = self.interface_config.get("index_fields", ["trade_date", "exchange"])
        
        # 检查多WAN口支持
        wan_config = config_manager.get_wan_config()
        self.port_allocator = port_allocator
        
        if wan_config.get("enabled", False):
            available_indices = self.port_allocator.get_available_wan_indices()
            wan_count = len(available_indices)
            if wan_count > 0:
                logger.info(f"已启用多WAN接口支持，WAN接口数量: {wan_count}")
            else:
                logger.warning("已启用多WAN接口支持，但未找到可用WAN接口")
        else:
            logger.warning("未启用多WAN接口支持，将使用系统默认网络接口")
    
    def _load_interface_config(self) -> Dict[str, Any]:
        """加载接口配置文件"""
        config_path = os.path.join(self.interface_dir, self.interface_name)
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    interface_config = json.load(f)
                    logger.debug(f"成功加载接口配置: {self.interface_name}")
                    return interface_config
        except Exception as e:
            logger.error(f"加载接口配置失败 {self.interface_name}: {str(e)}")
            
        logger.warning(f"接口配置文件不存在: {config_path}，将使用默认配置")
        return {
            "description": "中国A股交易日历",
            "api_name": "trade_cal",
            "fields": [],
            "params": {},
            "index_fields": ["trade_date", "exchange"],
            "available_fields": [
                "exchange",
                "trade_date",
                "is_open",
                "pretrade_date"
            ]
        }
    
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
            retry_count = 5
            port = None
            
            while retry_count > 0 and port is None:
                port = self.port_allocator.allocate_port(wan_idx)
                if port:
                    logger.debug(f"使用WAN接口 {wan_idx}，本地端口 {port}")
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
    
    def fetch_trade_calendar(self) -> Optional[pd.DataFrame]:
        """
        获取交易日历数据
        
        Returns:
            交易日历数据DataFrame，如果失败则返回None
        """
        try:
            # 准备参数
            api_name = self.interface_config.get("api_name", "trade_cal")
            
            # 设置API参数：交易所和日期范围
            params = {
                "exchange": self.exchange,
                "start_date": self.start_date,
                "end_date": self.end_date
            }
            
            # 使用接口配置中的available_fields作为请求字段
            fields = self.available_fields
            if not fields:
                logger.warning("接口配置中未定义available_fields，将获取所有字段")
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket()
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.info(f"正在从湘财Tushare获取交易日历数据...")
            logger.info(f"数据范围：{self.start_date} 至 {self.end_date}，交易所：{self.exchange}")
            logger.info(f"请求字段：{fields}")
            
            if use_wan:
                wan_idx, port = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
            
            start_time = time.time()
            
            # 增加超时，设置为120秒
            self.client.set_timeout(120)
            logger.info(f"增加API请求超时时间为120秒，提高网络可靠性")
            
            # 添加异常捕获，以便更好地调试
            try:
                # 如果使用WAN接口，设置本地地址绑定
                if use_wan:
                    wan_idx, port = wan_info
                    self.client.set_local_address('0.0.0.0', port)
                
                df = self.client.get_data(api_name=api_name, params=params, fields=fields)
                
                # 重置客户端设置
                if use_wan:
                    self.client.reset_local_address()
            except Exception as e:
                import traceback
                logger.error(f"获取API数据时发生异常: {str(e)}")
                logger.debug(f"异常详情: {traceback.format_exc()}")
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    
                return None
            
            # 数据验证和处理
            if df is not None and not df.empty:
                logger.success(f"成功获取数据，行数: {len(df)}, 列数: {df.shape[1]}")
                
                # 检查返回的数据包含的字段
                logger.info(f"API返回的字段: {list(df.columns)}")
                
                # 确保返回字段与接口配置中的字段匹配
                # 但不做字段转换，保留API返回的原始字段名
                missing_fields = [field for field in self.index_fields if field not in df.columns]
                if missing_fields:
                    logger.warning(f"API返回的数据缺少索引字段: {missing_fields}，这可能会影响数据存储")
                    
                    # 如果缺少exchange字段但我们知道交易所，则添加
                    if "exchange" in missing_fields and "exchange" not in df.columns:
                        df["exchange"] = self.exchange
                        logger.info(f"已添加默认交易所字段: {self.exchange}")
                
                # 确保日期字段是字符串格式
                if "trade_date" in df.columns and df["trade_date"].dtype != "object":
                    df["trade_date"] = df["trade_date"].astype(str)
                    logger.debug("已将trade_date字段转换为字符串格式")
                
                if self.verbose:
                    logger.debug(f"数据示例：\n{df.head(3)}")
                    
            elapsed = time.time() - start_time
            
            if df is None or df.empty:
                logger.error("数据为空")
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    
                return None
            
            # 释放WAN端口（如果使用了）
            if use_wan:
                wan_idx, port = wan_info
                self.port_allocator.release_port(wan_idx, port)
            
            logger.success(f"成功获取 {len(df)} 条交易日历数据，耗时 {elapsed:.2f}s")
            return df
            
        except Exception as e:
            logger.error(f"获取交易日历数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 如果有WAN信息，确保释放端口
            try:
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    logger.debug(f"已释放WAN {wan_idx} 的端口 {port}")
            except Exception as release_error:
                logger.warning(f"释放WAN端口时出错: {str(release_error)}")
                
            return None
    
    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将数据保存到MongoDB
        
        Args:
            df: 待保存的DataFrame
            
        Returns:
            是否成功保存
        """
        logger.info(f"保存数据到MongoDB数据库：{self.db_name}，集合：{self.collection_name}")
        
        if df is None or df.empty:
            logger.warning("没有数据可保存到MongoDB")
            return False
            
        try:
            # 将DataFrame转换为记录列表
            records = df.to_dict('records')
            
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试重新连接...")
                if not mongodb_handler.connect():
                    logger.error("重新连接MongoDB失败")
                    return False
            
            # 批量保存数据的统计信息
            stats = {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0}
            
            # 获取现有记录的trade_date集合，用于判断是更新还是插入
            query = {
                "trade_date": {"$gte": self.start_date, "$lte": self.end_date},
                "exchange": self.exchange
            }
            
            try:
                start_time = time.time()
                
                # 获取已存在的交易日历记录
                existing_records = mongodb_handler.find_documents(self.collection_name, query)
                # 建立字典便于查找和比较现有记录
                existing_records_dict = {record["trade_date"]: record for record in existing_records}
                existing_dates = set(existing_records_dict.keys())
                logger.info(f"在日期范围内找到 {len(existing_dates)} 条已存在的交易日历记录")
                
                # 批量插入新数据
                logger.info(f"开始批量处理 {len(records)} 条记录...")
                
                # 分离新增数据和更新数据
                new_records = []
                update_operations = []
                
                for record in records:
                    if "trade_date" in record and record["trade_date"] in existing_dates:
                        # 检查记录是否有实际变化（需要更新）
                        existing_record = existing_records_dict[record["trade_date"]]
                        need_update = False
                        
                        # 比较所有字段是否有变化
                        for key, value in record.items():
                            if key not in existing_record or existing_record[key] != value:
                                need_update = True
                                break
                        
                        if need_update:
                            # 构建更新操作
                            update_operations.append(
                                pymongo.UpdateOne(
                                    {"trade_date": record["trade_date"], "exchange": record.get("exchange", self.exchange)},
                                    {"$set": record}
                                )
                            )
                        else:
                            # 记录没有变化，统计为跳过
                            stats["skipped"] += 1
                    else:
                        new_records.append(record)
                
                logger.info(f"处理统计: 新记录:{len(new_records)}条, 需更新:{len(update_operations)}条, 无变化跳过:{stats['skipped']}条")
                
                # 批量插入新记录
                if new_records:
                    try:
                        # 使用mongodb_handler的批量插入方法
                        result_ids = mongodb_handler.insert_many_documents(self.collection_name, new_records, ordered=False)
                        stats["inserted"] = len(result_ids)
                        logger.info(f"已批量插入 {stats['inserted']} 条新记录")
                    except pymongo.errors.BulkWriteError as e:
                        # 处理部分插入错误
                        stats["inserted"] = e.details.get('nInserted', 0)
                        failed = len(new_records) - stats["inserted"]
                        stats["failed"] += failed
                        logger.warning(f"批量插入过程中发生 {failed} 条记录写入失败")
                
                # 批量更新记录
                if update_operations:
                    try:
                        # 使用mongodb_handler的批量写入方法
                        result = mongodb_handler.bulk_write(self.collection_name, update_operations, ordered=False)
                        # matched_count表示匹配的记录数，modified_count表示实际修改的记录数
                        matched = result.matched_count
                        modified = result.modified_count
                        stats["updated"] = modified
                        
                        if matched > modified:
                            # 有匹配但未修改的记录，这些记录实际上是完全相同的
                            stats["skipped"] += (matched - modified)
                            logger.debug(f"有 {matched - modified} 条记录匹配但未修改(内容相同)")
                            
                        logger.info(f"已批量更新 {stats['updated']} 条记录")
                    except pymongo.errors.BulkWriteError as e:
                        # 处理部分更新错误
                        stats["updated"] = e.details.get('nModified', 0)
                        failed = len(update_operations) - stats["updated"]
                        stats["failed"] += failed
                        logger.warning(f"批量更新过程中发生 {failed} 条记录更新失败")
                
                # 验证是否有数据被实际写入
                try:
                    count = mongodb_handler.count_documents(self.collection_name, query)
                    logger.info(f"查询到 {count} 条已存储的记录")
                    
                    # 记录操作耗时
                    elapsed = time.time() - start_time
                    logger.info(f"数据存储操作完成，耗时: {elapsed:.2f}秒")
                    
                    # 随机检查一条记录
                    if stats["inserted"] > 0 or stats["updated"] > 0:
                        sample_record = random.choice([r for r in records if all(field in r for field in self.index_fields)])
                        query = {field: sample_record[field] for field in self.index_fields}
                        found = mongodb_handler.find_document(self.collection_name, query)
                        if found:
                            logger.debug(f"验证写入成功：找到记录 {query}")
                        else:
                            logger.warning(f"验证写入失败：未找到记录 {query}")
                except Exception as e:
                    logger.warning(f"验证写入时出错: {str(e)}")
                
                # 总结数据保存结果
                logger.success(f"数据处理完成: 新插入 {stats['inserted']} 条记录，"
                              f"更新 {stats['updated']} 条记录，"
                              f"跳过 {stats['skipped']} 条记录，"
                              f"失败 {stats['failed']} 条记录")
                
                # 如果没有错误发生，就认为操作成功，即使没有实际更新记录
                if stats["failed"] == 0:
                    return True
                # 老的逻辑保留：如果有数据被插入或更新，也返回成功
                elif stats["inserted"] > 0 or stats["updated"] > 0:
                    return True
                else:
                    return False
                
            except Exception as e:
                logger.error(f"保存数据过程中出错: {str(e)}")
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
                return False
                
        except Exception as e:
            logger.error(f"保存到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def _check_indexes_created(self, collection):
        """检查索引是否已经创建"""
        try:
            # 获取集合上的索引
            indexes = list(mongodb_handler.list_indexes(self.collection_name))
            index_names = [idx.get('name') for idx in indexes]
            
            logger.info(f"集合 {self.collection_name} 现有索引: {index_names}")
            
            # 检查是否有复合索引
            index_fields = self.index_fields
            if index_fields:
                index_name = "_".join([f"{field}_1" for field in index_fields])
                if index_name in index_names:
                    logger.info(f"索引 {index_name} 已存在")
                else:
                    logger.warning(f"警告：索引 {index_name} 不存在，数据可能未被正确索引")
        except Exception as e:
            logger.warning(f"检查索引是否创建时出错: {str(e)}")
    
    def run(self) -> bool:
        """
        运行数据获取和保存流程
        
        Returns:
            是否成功
        """
        try:
            # 第一步：检查并确保集合和索引存在
            logger.info("第一步：检查并确保MongoDB集合和索引存在")
            if not self._ensure_collection_and_indexes():
                logger.error("无法确保MongoDB集合和索引，放弃数据获取")
                return False
            
            # 第二步：获取数据
            logger.info("第二步：从湘财Tushare获取交易日历数据")
            df = self.fetch_trade_calendar()
            if df is None or df.empty:
                logger.error("获取交易日历数据失败")
                return False
                
            # 第三步：保存数据到MongoDB
            logger.info("第三步：将交易日历数据保存到MongoDB")
            success = self.save_to_mongodb(df)
            
            return success
            
        except Exception as e:
            logger.error(f"运行过程中发生异常: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
            
    def _ensure_collection_and_indexes(self) -> bool:
        """
        确保MongoDB集合存在并具有正确的索引
        
        Returns:
            bool: 是否成功确保集合和索引
        """
        try:
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.info("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return False
                    
            logger.info(f"检查MongoDB集合 {self.collection_name} 是否存在")
            
            # 使用新的collection_exists和create_collection方法
            if not mongodb_handler.collection_exists(self.collection_name):
                logger.info(f"集合 {self.collection_name} 不存在，正在创建...")
                mongodb_handler.create_collection(self.collection_name)
                logger.info(f"集合 {self.collection_name} 已成功创建")
            else:
                logger.info(f"集合 {self.collection_name} 已存在")
            
            # 检查并创建索引
            collection = mongodb_handler.get_collection(self.collection_name)
            if collection is None:
                logger.error(f"无法获取集合: {self.collection_name}")
                return False
                
            # 检查并确保索引
            return self._ensure_collection_indexes(collection)
                
        except Exception as e:
            logger.error(f"确保集合和索引时出错: {str(e)}")
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
                # 查询交易日期
                query = {
                    "trade_date": {"$gte": start_date, "$lte": end_date}
                }
                
                # 查询结果
                results = mongodb_handler.find_documents("trade_cal", query)
                
                # 提取日期列表并排序
                if results:
                    trade_dates = [doc.get("trade_date") for doc in results if "trade_date" in doc]
                    trade_dates.sort()
                    logger.info(f"从trade_cal集合获取到 {len(trade_dates)} 个交易日")
                else:
                    trade_dates = []
                
                if not trade_dates:
                    logger.warning(f"未从trade_cal集合获取到日期范围 {start_date} 至 {end_date} 内的交易日，将使用日期范围内的所有日期")
                    # 生成日期范围内的所有日期
                    return self._generate_date_range(start_date, end_date)
                
                return trade_dates
                
            except Exception as e:
                logger.error(f"获取交易日历失败: {str(e)}")
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
                
                # 出错时生成日期范围内的所有日期作为备选
                return self._generate_date_range(start_date, end_date)
                
        except Exception as e:
            logger.error(f"获取交易日历失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 兜底方案：生成日期范围内的所有日期
            return self._generate_date_range(start_date, end_date)
    
    def _generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """
        生成日期范围内的所有日期
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            日期列表，格式为YYYYMMDD
        """
        start_date_obj = datetime.strptime(start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(end_date, '%Y%m%d')
        
        trade_dates = []
        current_date = start_date_obj
        while current_date <= end_date_obj:
            trade_dates.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        
        logger.info(f"生成日期范围内的 {len(trade_dates)} 个日期")
        return trade_dates

    def _verify_mongodb_connection(self):
        """验证MongoDB连接和写入权限"""
        try:
            # 尝试ping命令来验证连接和权限
            if mongodb_handler.is_connected():
                db = mongodb_handler.get_database()
                # 使用ping命令验证连接
                db.command('ping')
                logger.debug(f"MongoDB连接和写入权限验证成功")
            else:
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("MongoDB连接失败")
        except Exception as e:
            logger.error(f"MongoDB连接或写入权限验证失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")

    def _ensure_collection_indexes(self, collection):
        """确保集合有适当的索引"""
        try:
            # 使用接口配置中的index_fields作为索引字段
            index_fields = self.index_fields
            
            # 从配置文件获取索引字段
            if not index_fields:
                # 尝试从接口配置文件重新加载索引字段
                interface_path = os.path.join(self.interface_dir, self.interface_name)
                try:
                    with open(interface_path, 'r', encoding='utf-8') as f:
                        interface_config = json.load(f)
                        index_fields = interface_config.get("index_fields", ["trade_date", "exchange"])
                        logger.info(f"从接口配置文件加载索引字段: {index_fields}")
                except Exception as e:
                    logger.warning(f"从接口配置文件加载索引字段失败: {str(e)}")
                    index_fields = ["trade_date", "exchange"]
                    logger.warning(f"使用默认索引: {index_fields}")
            
            logger.info(f"为集合 {self.collection_name} 设置索引字段: {index_fields}")
            
            # 检查索引是否已存在
            try:
                # 使用collection.index_information()获取更详细的索引信息
                existing_indexes = collection.index_information()
                logger.debug(f"现有索引信息: {existing_indexes}")
                
                # 删除多余的单字段索引
                if 'trade_date_single_idx' in existing_indexes:
                    logger.info("发现多余的单字段索引，正在删除...")
                    try:
                        collection.drop_index('trade_date_single_idx')
                        logger.success("成功删除多余的单字段索引 trade_date_single_idx")
                    except Exception as e:
                        logger.warning(f"删除多余索引时出错: {str(e)}")
                
                # 创建复合索引
                if len(index_fields) > 0:
                    # 构建索引名称
                    index_name = "_".join([f"{field}_1" for field in index_fields])
                    
                    # 检查索引是否已存在
                    if index_name in existing_indexes:
                        logger.debug(f"复合索引 {index_fields} 已存在，跳过创建")
                    else:
                        # 索引不存在，创建它
                        logger.info(f"正在为集合 {self.collection_name} 创建复合唯一索引 {index_fields}...")
                        try:
                            # 使用传入的collection对象创建索引
                            collection.create_index(
                                [(field, 1) for field in index_fields],
                                unique=True,
                                background=True,
                                name=index_name
                            )
                            logger.info(f"已为字段 {index_fields} 创建唯一复合索引: {index_name}")
                        except Exception as e:
                            logger.error(f"创建索引时出错: {str(e)}")
                            return False
                
                # 移除创建单字段索引的代码，避免创建不必要的索引
            except Exception as e:
                logger.warning(f"获取索引信息时出错: {str(e)}")
                # 尝试直接创建索引
                try:
                    # 使用传入的collection对象创建索引
                    collection.create_index(
                        [(field, 1) for field in index_fields],
                        unique=True,
                        background=True,
                        name="_".join([f"{field}_1" for field in index_fields])
                    )
                    logger.info(f"已为字段 {index_fields} 创建唯一复合索引")
                except Exception as e:
                    logger.error(f"创建索引时出错: {str(e)}")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"创建索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取交易日历数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--interface-name', default='trade_cal.json', help='接口配置文件名称')
    parser.add_argument('--exchange', default='SSE', help='交易所代码：SSE-上交所, SZSE-深交所')
    parser.add_argument('--start-date', help='开始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式：YYYYMMDD')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='trade_cal', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--list-fields', action='store_true', help='列出接口定义的字段')
    parser.add_argument('--dry-run', action='store_true', help='仅运行流程，不保存数据')
    args = parser.parse_args()
    
    try:
        # 如果只是列出字段，则加载接口配置并显示字段
        if args.list_fields:
            # 加载接口配置
            interface_path = os.path.join(args.interface_dir, args.interface_name)
            try:
                with open(interface_path, 'r', encoding='utf-8') as f:
                    interface_config = json.load(f)
                    
                available_fields = interface_config.get("available_fields", [])
                index_fields = interface_config.get("index_fields", [])
                
                print(f"\n接口定义文件: {interface_path}")
                print(f"可用字段: {available_fields}")
                print(f"索引字段: {index_fields}\n")
                
                return
            except Exception as e:
                logger.error(f"加载接口配置失败: {str(e)}")
                sys.exit(1)
        
        # 创建获取器并运行
        fetcher = TradeCalFetcher(
            config_path=args.config,
            interface_dir=args.interface_dir,
            interface_name=args.interface_name,
            exchange=args.exchange,
            start_date=args.start_date,
            end_date=args.end_date,
            db_name=args.db_name,
            collection_name=args.collection_name,
            verbose=args.verbose
        )
        
        # 干运行模式，不保存数据
        if args.dry_run:
            logger.info("干运行模式，仅检查集合和索引，获取数据但不保存")
            
            # 检查集合和索引
            if not fetcher._ensure_collection_and_indexes():
                logger.error("检查集合和索引失败")
                sys.exit(1)
                
            # 获取数据
            df = fetcher.fetch_trade_calendar()
            if df is not None and not df.empty:
                logger.success(f"成功获取 {len(df)} 条数据，但不保存到数据库")
                if args.verbose:
                    print("\n数据示例：")
                    print(df.head(5))
                success = True
            else:
                logger.error("获取数据失败")
                success = False
        else:
            # 正常运行模式
            success = fetcher.run()
        
        if success:
            logger.success("交易日历数据" + ("获取成功" if args.dry_run else "获取和保存成功"))
            sys.exit(0)
        else:
            logger.error("交易日历数据" + ("获取失败" if args.dry_run else "获取或保存失败"))
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"运行过程中发生异常: {str(e)}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()