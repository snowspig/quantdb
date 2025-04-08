#!/usr/bin/env python
"""
Tushare数据获取器基类 - 提供通用功能

该模块提供了一个基础类，用于从湘财Tushare获取数据并保存到MongoDB
实现了常见的功能如WAN口管理、MongoDB连接和索引管理、数据保存等

子类需要实现具体的获取逻辑
"""
import os
import sys
import json
import time
import pandas as pd
import pymongo
import random
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple
from pathlib import Path
from loguru import logger

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core import config_manager, mongodb_handler
from core.wan_manager import port_allocator
from core.tushare_client_wan import TushareClientWAN

class TushareFetcher:
    """
    湘财Tushare数据获取器基类
    
    提供通用的数据获取、处理和存储功能，包括：
    1. 配置加载
    2. MongoDB连接和索引管理
    3. 多WAN口支持
    4. 数据保存（增量更新）
    
    子类需要实现以下方法：
    - fetch_data: 从数据源获取数据
    """
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = None,
        db_name: str = None,
        collection_name: str = None,
        verbose: bool = False
    ):
        """
        初始化数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.collection_name = collection_name
        self.verbose = verbose

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
        self.index_fields = self.interface_config.get("index_fields", [])
        
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
        if not self.interface_name:
            logger.warning("未指定接口名称，将使用默认配置")
            return {}
            
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
        return {}

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
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从数据源获取数据 - 子类需要实现该方法
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            获取的数据DataFrame，如果失败则返回None
        """
        raise NotImplementedError("子类必须实现fetch_data方法")
    
    def save_to_mongodb(self, df: pd.DataFrame, unique_keys: List[str] = None) -> bool:
        """
        将数据保存到MongoDB
        
        Args:
            df: 待保存的DataFrame
            unique_keys: 用于判断记录是否存在的唯一键列表，默认使用接口配置中的index_fields
            
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
            
            # 使用传入的unique_keys或接口配置中的index_fields
            if unique_keys is None:
                unique_keys = self.index_fields
                
            if not unique_keys:
                logger.warning("未指定唯一键，将使用_id作为唯一键")
                unique_keys = ["_id"]
            
            # 批量保存数据的统计信息
            stats = {"inserted": 0, "updated": 0, "skipped": 0, "failed": 0}
            
            try:
                start_time = time.time()
                
                # 查询现有记录
                existing_records = []
                query_conditions = []
                
                # 构建查询条件，获取所有可能存在的记录
                for record in records:
                    query = {}
                    valid = True
                    for key in unique_keys:
                        if key in record and record[key] is not None:
                            query[key] = record[key]
                        else:
                            valid = False
                            break
                    
                    if valid and query:
                        query_conditions.append(query)
                
                # 如果有查询条件，获取现有记录
                if query_conditions:
                    existing_records = mongodb_handler.find_documents(
                        self.collection_name, 
                        {"$or": query_conditions}
                    )
                
                # 建立字典便于查找和比较现有记录
                existing_records_dict = {}
                for record in existing_records:
                    # 构建复合键作为字典键
                    key_values = tuple(str(record.get(key, '')) for key in unique_keys if key in record)
                    if key_values:
                        existing_records_dict[key_values] = record
                
                logger.debug(f"找到 {len(existing_records_dict)} 条已存在的记录")
                
                # 批量处理数据
                logger.debug(f"开始批量处理 {len(records)} 条记录...")
                
                # 分离新增数据和更新数据
                new_records = []
                update_operations = []
                
                for record in records:
                    # 构建复合键
                    key_values = tuple(str(record.get(key, '')) for key in unique_keys if key in record)
                    
                    if key_values and key_values in existing_records_dict:
                        # 检查记录是否有实际变化（需要更新）
                        existing_record = existing_records_dict[key_values]
                        need_update = False
                        
                        # 比较所有字段是否有变化
                        for key, value in record.items():
                            if key not in existing_record or existing_record[key] != value:
                                need_update = True
                                break
                        
                        if need_update:
                            # 构建更新操作
                            query = {key: record[key] for key in unique_keys if key in record}
                            update_operations.append(
                                pymongo.UpdateOne(
                                    query,
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
                    count = mongodb_handler.count_documents(self.collection_name, {})
                    logger.debug(f"查询到 {count} 条已存储的记录")
                    
                    # 记录操作耗时
                    elapsed = time.time() - start_time
                    logger.info(f"数据存储操作完成，耗时: {elapsed:.2f}秒")
                    
                    # 随机检查一条记录
                    if stats["inserted"] > 0 or stats["updated"] > 0:
                        # 找出所有具有完整唯一键的记录
                        valid_records = [r for r in records if all(k in r for k in unique_keys)]
                        if valid_records:
                            sample_record = random.choice(valid_records)
                            query = {key: sample_record[key] for key in unique_keys}
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
                # 如果有数据被插入或更新，也返回成功
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
            logger.info(f"第二步：从湘财Tushare获取{self.collection_name}数据")
            df = self.fetch_data()
            if df is None or df.empty:
                logger.error("获取数据失败")
                return False
                
            # 第三步：处理和过滤数据（子类可能需要重写此步骤）
            logger.info("第三步：处理和过滤数据")
            processed_df = self.process_data(df) if hasattr(self, 'process_data') else df
            if processed_df.empty:
                logger.warning("处理后没有有效数据")
                return False
                
            # 第四步：保存数据到MongoDB
            logger.info(f"第四步：将数据保存到MongoDB集合{self.collection_name}")
            success = self.save_to_mongodb(processed_df)
            
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
            
            # 使用collection_exists和create_collection方法
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
    
    def _ensure_collection_indexes(self, collection) -> bool:
        """
        确保集合有适当的索引
        
        Args:
            collection: MongoDB集合对象
            
        Returns:
            bool: 是否成功创建或验证索引
        """
        try:
            # 使用接口配置中的index_fields作为索引字段
            index_fields = self.index_fields
            
            # 如果索引字段为空，返回
            if not index_fields:
                logger.warning("未配置索引字段，跳过索引创建")
                return True
            
            logger.info(f"为集合 {self.collection_name} 设置索引字段: {index_fields}")
            
            # 检查索引是否已存在
            try:
                # 使用collection.index_information()获取更详细的索引信息
                existing_indexes = collection.index_information()
                logger.debug(f"现有索引信息: {existing_indexes}")
                
                # 创建复合唯一索引
                if len(index_fields) > 0:
                    # 构建索引名称
                    index_name = "_".join([f"{field}_1" for field in index_fields])
                    
                    # 检查索引是否已存在
                    if index_name in existing_indexes:
                        logger.debug(f"复合索引 {index_fields} 已存在，检查是否为唯一索引")
                        
                        # 检查是否是唯一索引
                        is_unique = existing_indexes[index_name].get('unique', False)
                        if not is_unique:
                            # 索引存在但不是唯一索引，删除并重新创建
                            logger.info(f"复合索引 {index_fields} 存在但不是唯一索引，重新创建...")
                            try:
                                collection.drop_index(index_name)
                                collection.create_index(
                                    [(field, 1) for field in index_fields],
                                    unique=True,
                                    background=True,
                                    name=index_name
                                )
                                logger.info(f"已为字段 {index_fields} 重新创建唯一复合索引")
                            except Exception as e:
                                logger.error(f"重新创建索引时出错: {str(e)}")
                    else:
                        # 索引不存在，创建它
                        logger.info(f"正在为集合 {self.collection_name} 创建复合唯一索引 {index_fields}...")
                        try:
                            collection.create_index(
                                [(field, 1) for field in index_fields],
                                unique=True,
                                background=True,
                                name=index_name
                            )
                            logger.info(f"已为字段 {index_fields} 创建唯一复合索引")
                        except Exception as e:
                            logger.error(f"创建索引时出错: {str(e)}")
                            return False
                
                # 为每个字段单独创建索引以加速单字段查询
                for field in index_fields:
                    index_name = f"{field}_1"
                    if index_name not in existing_indexes:
                        logger.info(f"正在为字段 {field} 创建单字段索引...")
                        try:
                            collection.create_index(
                                [(field, 1)],
                                background=True,
                                name=index_name
                            )
                            logger.info(f"已为字段 {field} 创建单字段索引")
                        except Exception as e:
                            logger.warning(f"创建单字段索引时出错: {str(e)}")
                    else:
                        logger.debug(f"字段 {field} 的单字段索引已存在，跳过创建")
                
            except Exception as e:
                logger.warning(f"获取索引信息时出错: {str(e)}")
                # 尝试直接创建索引
                try:
                    for field in index_fields:
                        collection.create_index(
                            [(field, 1)],
                            background=True
                        )
                        logger.info(f"已为字段 {field} 创建单字段索引")
                        
                    # 创建复合唯一索引
                    if len(index_fields) > 1:
                        collection.create_index(
                            [(field, 1) for field in index_fields],
                            unique=True,
                            background=True
                        )
                        logger.info(f"已为字段 {index_fields} 创建唯一复合索引")
                except Exception as e:
                    logger.error(f"创建索引时出错: {str(e)}")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"确保索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False 