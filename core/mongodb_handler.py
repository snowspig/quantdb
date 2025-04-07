#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MongoDB处理器模块
提供统一的MongoDB数据库操作接口，包括连接管理、CRUD操作和索引处理
"""
import os
import sys
import logging
import pymongo
from pymongo import MongoClient, errors
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, DeleteResult
from bson.objectid import ObjectId
from typing import Dict, List, Any, Optional, Union, Tuple

# 导入配置管理器
from core.config_manager import config_manager

class MongoDBHandler:
    """
    MongoDB处理器类
    提供统一的MongoDB数据库操作接口，包括：
    1. 数据库连接管理
    2. 集合操作（创建、删除）
    3. 文档操作（增删改查）
    4. 索引管理
    """
    
    _instance = None
    
    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(MongoDBHandler, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化MongoDB处理器"""
        # 避免重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        # 设置日志
        self.logger = logging.getLogger("core.MongoDBHandler")
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(console_handler)
            
            # 设置日志级别
            log_level = os.environ.get("QUANTDB_LOG_LEVEL", "INFO").upper()
            self.logger.setLevel(getattr(logging, log_level, logging.INFO))
            
        # 初始化连接
        self.client = None
        self.db = None
        
        # 读取MongoDB配置
        self.mongo_config = config_manager.get_mongodb_config()
        
        # 连接到MongoDB
        self.connect()
        
        # 标记为已初始化
        self._initialized = True
        
    def connect(self):
        """
        连接到MongoDB服务器
        
        Returns:
            bool: 是否成功连接
        """
        try:
            # 获取连接信息
            uri = self.mongo_config.get('uri', 'mongodb://localhost:27017/')
            db_name = self.mongo_config.get('db', 'quantdb')
            
            # 建立连接
            self.client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            
            # 测试连接
            self.client.admin.command('ismaster')
            
            # 获取数据库
            self.db = self.client[db_name]
            
            self.logger.info(f"成功连接到MongoDB: {uri}, 数据库: {db_name}")
            return True
            
        except errors.ServerSelectionTimeoutError:
            self.logger.error("MongoDB服务器连接超时，请检查服务是否运行")
            return False
        except errors.OperationFailure as e:
            self.logger.error(f"MongoDB操作失败: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"连接MongoDB失败: {str(e)}")
            return False
            
    def reconnect(self):
        """
        重新连接到MongoDB
        
        Returns:
            bool: 是否成功重连
        """
        try:
            if self.client:
                self.client.close()
                
            return self.connect()
            
        except Exception as e:
            self.logger.error(f"重新连接MongoDB失败: {str(e)}")
            return False
            
    def is_connected(self) -> bool:
        """
        检查是否已连接到MongoDB
        
        Returns:
            bool: 连接状态
        """
        if not self.client:
            return False
            
        try:
            # 使用ping命令测试连接
            self.client.admin.command('ping')
            return True
        except Exception:
            return False
            
    def get_database(self) -> Optional[Database]:
        """
        获取当前数据库对象
        
        Returns:
            Optional[Database]: 数据库对象，未连接时返回None
        """
        return self.db if self.is_connected() else None
        
    def get_collection(self, collection_name: str) -> Optional[Collection]:
        """
        获取指定集合
        
        Args:
            collection_name: 集合名称
            
        Returns:
            Optional[Collection]: 集合对象，未连接时返回None
        """
        if not self.is_connected():
            self.logger.warning(f"MongoDB未连接，无法获取集合: {collection_name}")
            return None
            
        return self.db[collection_name]
        
    def create_collection(self, collection_name: str, **options) -> bool:
        """
        创建集合
        
        Args:
            collection_name: 集合名称
            **options: 创建选项
            
        Returns:
            bool: 是否成功创建
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法创建集合: {collection_name}")
                return False
                
            # 检查集合是否已存在
            if collection_name in self.db.list_collection_names():
                self.logger.info(f"集合已存在: {collection_name}")
                return True
                
            # 创建集合
            self.db.create_collection(collection_name, **options)
            self.logger.info(f"集合创建成功: {collection_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"创建集合失败: {collection_name}, 错误: {str(e)}")
            return False
            
    def drop_collection(self, collection_name: str) -> bool:
        """
        删除集合
        
        Args:
            collection_name: 集合名称
            
        Returns:
            bool: 是否成功删除
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法删除集合: {collection_name}")
                return False
                
            # 检查集合是否存在
            if collection_name not in self.db.list_collection_names():
                self.logger.info(f"集合不存在，无需删除: {collection_name}")
                return True
                
            # 删除集合
            self.db.drop_collection(collection_name)
            self.logger.info(f"集合已删除: {collection_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"删除集合失败: {collection_name}, 错误: {str(e)}")
            return False
            
    def insert_one(self, collection_name: str, document: Dict) -> str:
        """
        插入单个文档
        
        Args:
            collection_name: 集合名称
            document: 要插入的文档
            
        Returns:
            str: 插入的文档ID，失败时返回空字符串
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法插入文档到集合: {collection_name}")
                return ""
                
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            
            if result.acknowledged:
                return str(result.inserted_id)
            else:
                return ""
                
        except Exception as e:
            self.logger.error(f"插入文档失败: {collection_name}, 错误: {str(e)}")
            return ""
            
    def insert_many(self, collection_name: str, documents: List[Dict], ordered: bool = True) -> List[str]:
        """
        插入多个文档
        
        Args:
            collection_name: 集合名称
            documents: 要插入的文档列表
            ordered: 是否按顺序插入
            
        Returns:
            List[str]: 插入的文档ID列表
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法插入多个文档到集合: {collection_name}")
                return []
                
            collection = self.db[collection_name]
            result = collection.insert_many(documents, ordered=ordered)
            
            if result.acknowledged:
                return [str(id) for id in result.inserted_ids]
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"插入多个文档失败: {collection_name}, 错误: {str(e)}")
            return []
            
    def find_one(self, collection_name: str, query: Dict = None, projection: Dict = None) -> Optional[Dict]:
        """
        查询单个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            projection: 投影配置
            
        Returns:
            Optional[Dict]: 查询到的文档，未找到时返回None
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法查询文档: {collection_name}")
                return None
                
            collection = self.db[collection_name]
            result = collection.find_one(query or {}, projection=projection)
            return result
            
        except Exception as e:
            self.logger.error(f"查询文档失败: {collection_name}, 错误: {str(e)}")
            return None
            
    def find(self, collection_name: str, query: Dict = None, projection: Dict = None, 
             sort: List = None, limit: int = 0, skip: int = 0) -> List[Dict]:
        """
        查询多个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            projection: 投影配置
            sort: 排序配置，形如 [('field', 1)]
            limit: 返回结果数量限制
            skip: 跳过的结果数量
            
        Returns:
            List[Dict]: 查询到的文档列表
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法查询多个文档: {collection_name}")
                return []
                
            collection = self.db[collection_name]
            cursor = collection.find(query or {}, projection=projection)
            
            if sort:
                cursor = cursor.sort(sort)
                
            if skip:
                cursor = cursor.skip(skip)
                
            if limit:
                cursor = cursor.limit(limit)
                
            return list(cursor)
            
        except Exception as e:
            self.logger.error(f"查询多个文档失败: {collection_name}, 错误: {str(e)}")
            return []
            
    def count_documents(self, collection_name: str, query: Dict = None) -> int:
        """
        计算文档数量
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            
        Returns:
            int: 文档数量
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法计数文档: {collection_name}")
                return 0
                
            collection = self.db[collection_name]
            return collection.count_documents(query or {})
            
        except Exception as e:
            self.logger.error(f"计数文档失败: {collection_name}, 错误: {str(e)}")
            return 0
            
    def update_one(self, collection_name: str, query: Dict, update: Dict, upsert: bool = False) -> int:
        """
        更新单个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新操作
            upsert: 不存在时是否插入
            
        Returns:
            int: 更新的文档数量
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法更新文档: {collection_name}")
                return 0
                
            collection = self.db[collection_name]
            result = collection.update_one(query, update, upsert=upsert)
            
            return result.modified_count
            
        except Exception as e:
            self.logger.error(f"更新文档失败: {collection_name}, 错误: {str(e)}")
            return 0
            
    def update_many(self, collection_name: str, query: Dict, update: Dict, upsert: bool = False) -> int:
        """
        更新多个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新操作
            upsert: 不存在时是否插入
            
        Returns:
            int: 更新的文档数量
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法更新多个文档: {collection_name}")
                return 0
                
            collection = self.db[collection_name]
            result = collection.update_many(query, update, upsert=upsert)
            
            return result.modified_count
            
        except Exception as e:
            self.logger.error(f"更新多个文档失败: {collection_name}, 错误: {str(e)}")
            return 0
            
    def delete_one(self, collection_name: str, query: Dict) -> int:
        """
        删除单个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            
        Returns:
            int: 删除的文档数量
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法删除文档: {collection_name}")
                return 0
                
            collection = self.db[collection_name]
            result = collection.delete_one(query)
            
            return result.deleted_count
            
        except Exception as e:
            self.logger.error(f"删除文档失败: {collection_name}, 错误: {str(e)}")
            return 0
            
    def delete_many(self, collection_name: str, query: Dict) -> int:
        """
        删除多个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            
        Returns:
            int: 删除的文档数量
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法删除多个文档: {collection_name}")
                return 0
                
            collection = self.db[collection_name]
            result = collection.delete_many(query)
            
            return result.deleted_count
            
        except Exception as e:
            self.logger.error(f"删除多个文档失败: {collection_name}, 错误: {str(e)}")
            return 0
            
    def create_index(self, collection_name: str, keys, **kwargs) -> str:
        """
        创建索引
        
        Args:
            collection_name: 集合名称
            keys: 索引键，可以是字符串或列表或字典
            **kwargs: 索引选项
            
        Returns:
            str: 创建的索引名称，失败时返回空字符串
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法创建索引: {collection_name}")
                return ""
                
            collection = self.db[collection_name]
            
            # 确保默认选项
            if 'background' not in kwargs:
                kwargs['background'] = True
                
            result = collection.create_index(keys, **kwargs)
            self.logger.info(f"索引创建成功: {collection_name}.{result}")
            return result
            
        except Exception as e:
            self.logger.error(f"创建索引失败: {collection_name}, 错误: {str(e)}")
            return ""
            
    def drop_index(self, collection_name: str, index_name: str) -> bool:
        """
        删除索引
        
        Args:
            collection_name: 集合名称
            index_name: 索引名称
            
        Returns:
            bool: 是否成功删除
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法删除索引: {collection_name}.{index_name}")
                return False
                
            collection = self.db[collection_name]
            collection.drop_index(index_name)
            self.logger.info(f"索引已删除: {collection_name}.{index_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"删除索引失败: {collection_name}.{index_name}, 错误: {str(e)}")
            return False
            
    def list_indexes(self, collection_name: str) -> List[Dict]:
        """
        列出集合的所有索引
        
        Args:
            collection_name: 集合名称
            
        Returns:
            List[Dict]: 索引信息列表
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法列出索引: {collection_name}")
                return []
                
            collection = self.db[collection_name]
            return list(collection.list_indexes())
            
        except Exception as e:
            self.logger.error(f"列出索引失败: {collection_name}, 错误: {str(e)}")
            return []
            
    def ensure_index(self, collection_name: str, keys, **kwargs) -> bool:
        """
        确保索引存在，不存在则创建
        
        Args:
            collection_name: 集合名称
            keys: 索引键
            **kwargs: 索引选项
            
        Returns:
            bool: 是否成功确保索引
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法确保索引: {collection_name}")
                return False
                
            # 检查索引是否已存在
            collection = self.db[collection_name]
            existing_indexes = list(collection.list_indexes())
            
            # 将keys转换为字典形式，便于比较
            if isinstance(keys, str):
                keys_dict = {keys: 1}
            elif isinstance(keys, list):
                keys_dict = {k: 1 for k in keys}
            else:
                keys_dict = keys
                
            # 检查是否已存在相同的索引
            for idx in existing_indexes:
                if idx.get('key') == keys_dict:
                    self.logger.debug(f"索引已存在: {collection_name}.{idx['name']}")
                    return True
                    
            # 创建索引
            result = self.create_index(collection_name, keys, **kwargs)
            return bool(result)
            
        except Exception as e:
            self.logger.error(f"确保索引失败: {collection_name}, 错误: {str(e)}")
            return False
            
    def bulk_write(self, collection_name: str, operations: List, ordered: bool = True) -> Dict:
        """
        批量写入操作
        
        Args:
            collection_name: 集合名称
            operations: 操作列表
            ordered: 是否按顺序执行
            
        Returns:
            Dict: 操作结果统计
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法执行批量写入: {collection_name}")
                return {'acknowledged': False}
                
            collection = self.db[collection_name]
            result = collection.bulk_write(operations, ordered=ordered)
            
            return {
                'acknowledged': result.acknowledged,
                'inserted_count': result.inserted_count,
                'modified_count': result.modified_count,
                'deleted_count': result.deleted_count,
                'upserted_count': result.upserted_count
            }
            
        except Exception as e:
            self.logger.error(f"批量写入失败: {collection_name}, 错误: {str(e)}")
            return {'acknowledged': False, 'error': str(e)}
            
    def distinct(self, collection_name: str, field: str, query: Dict = None) -> List:
        """
        获取字段的唯一值列表
        
        Args:
            collection_name: 集合名称
            field: 字段名
            query: 查询条件
            
        Returns:
            List: 唯一值列表
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法获取唯一值: {collection_name}.{field}")
                return []
                
            collection = self.db[collection_name]
            return collection.distinct(field, filter=query)
            
        except Exception as e:
            self.logger.error(f"获取唯一值失败: {collection_name}.{field}, 错误: {str(e)}")
            return []
            
    def find_and_modify(self, collection_name: str, query: Dict, update: Dict,
                         upsert: bool = False, sort: List = None, **kwargs) -> Optional[Dict]:
        """
        查找并修改文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新操作
            upsert: 不存在时是否插入
            sort: 排序方式
            **kwargs: 其他选项
            
        Returns:
            Optional[Dict]: 修改前或修改后的文档
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法查找并修改文档: {collection_name}")
                return None
                
            collection = self.db[collection_name]
            return collection.find_one_and_update(
                query, update, upsert=upsert, sort=sort, **kwargs
            )
            
        except Exception as e:
            self.logger.error(f"查找并修改文档失败: {collection_name}, 错误: {str(e)}")
            return None
            
    def create_standard_indexes(self, collection_name: str, field_mappings: Dict[str, str]) -> bool:
        """
        为集合创建标准索引
        根据字段类型创建适当的索引
        
        Args:
            collection_name: 集合名称
            field_mappings: 字段映射，格式为 {字段名: 字段类型}
                字段类型可以是：'date', 'code', 'number' 等
            
        Returns:
            bool: 是否成功创建所有索引
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法创建标准索引: {collection_name}")
                return False
                
            # 索引创建结果
            results = []
            
            # 根据字段类型创建索引
            for field, field_type in field_mappings.items():
                if field_type == 'date':
                    # 日期字段创建降序索引，便于按时间倒序查询
                    result = self.ensure_index(collection_name, [(field, -1)])
                    results.append(result)
                    
                elif field_type == 'code':
                    # 代码字段创建唯一索引
                    result = self.ensure_index(collection_name, field)
                    results.append(result)
                    
                elif field_type in ['number', 'float', 'int']:
                    # 数值字段创建索引
                    result = self.ensure_index(collection_name, field)
                    results.append(result)
            
            # 检查是否所有索引都创建成功
            success = all(results)
            if success:
                self.logger.info(f"已为集合 {collection_name} 创建所有标准索引")
            else:
                self.logger.warning(f"集合 {collection_name} 部分标准索引创建失败")
                
            return success
            
        except Exception as e:
            self.logger.error(f"创建标准索引失败: {collection_name}, 错误: {str(e)}")
            return False

    def ensure_time_series_collection(self, collection_name: str, time_field: str) -> bool:
        """
        确保时间序列集合存在
        如果集合不存在，创建一个新的时间序列集合
        
        Args:
            collection_name: 集合名称
            time_field: 时间字段名
            
        Returns:
            bool: 是否成功确保时间序列集合
        """
        try:
            if not self.is_connected():
                self.logger.warning(f"MongoDB未连接，无法确保时间序列集合: {collection_name}")
                return False
                
            # 检查集合是否存在
            if collection_name in self.db.list_collection_names():
                self.logger.info(f"集合已存在: {collection_name}")
                return True
                
            # 创建时间序列集合
            self.db.create_collection(
                collection_name,
                timeseries={
                    'timeField': time_field,
                    'metaField': 'metadata',
                    'granularity': 'minutes'
                },
                expireAfterSeconds=None  # 不自动过期
            )
            
            self.logger.info(f"已创建时间序列集合: {collection_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"确保时间序列集合失败: {collection_name}, 错误: {str(e)}")
            return False
            
    def close(self):
        """关闭MongoDB连接"""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB连接已关闭")


# 创建全局MongoDB处理器实例
mongodb_handler = MongoDBHandler()