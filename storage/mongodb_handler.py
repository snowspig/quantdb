"""
MongoDB处理器模块，提供统一的数据库操作接口
"""
import os
import sys
import time
import pymongo
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure, OperationFailure
from pymongo.cursor import Cursor
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, DeleteResult
from pymongo.collection import Collection
from pymongo.database import Database
from datetime import datetime, timedelta
import pandas as pd

# 导入配置管理器
from config import config_manager


class MongoDBHandler:
    """
    MongoDB处理器类，提供统一的数据库操作接口
    """
    
    _instance = None
    
    def __new__(cls, config_path=None, silent=False):
        """
        单例模式实现
        
        Args:
            config_path: 配置文件路径
            silent: 静默模式，不输出日志
        """
        if cls._instance is None:
            cls._instance = super(MongoDBHandler, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path=None, silent=False):
        """
        初始化MongoDB处理器
        
        Args:
            config_path: 配置文件路径，不提供则使用默认配置
            silent: 静默模式，不输出日志
        """
        # 避免重复初始化
        if self._initialized:
            return
            
        self.config_path = config_path
        self.silent = silent
        
        # 从配置中加载设置
        self.db_config = config_manager.get_mongodb_config()
        
        # 数据库配置
        self.host = self.db_config.get('host', 'localhost')
        self.port = self.db_config.get('port', 27017)
        self.username = self.db_config.get('username', '')
        self.password = self.db_config.get('password', '')
        self.database_name = self.db_config.get('database', 'quantdb')
        self.auth_source = self.db_config.get('auth_source', 'admin')
        
        # 连接选项
        self.options = self.db_config.get('options', {})
        
        # 初始化客户端和数据库连接
        self.client = None
        self.db = None
        self._connect()
        
        # 标记为已初始化
        self._initialized = True
        
        if not self.silent:
            logging.info(f"MongoDB处理器初始化完成，数据库: {self.database_name}")
    
    def _connect(self):
        """
        建立MongoDB连接
        
        Returns:
            bool: 是否成功连接
        """
        try:
            # 构建连接URI
            uri = "mongodb://"
            
            # 添加身份验证
            if self.username and self.password:
                uri += f"{self.username}:{self.password}@"
                
            uri += f"{self.host}:{self.port}/{self.database_name}"
            
            # 添加认证源
            if self.auth_source:
                uri += f"?authSource={self.auth_source}"
            
            # 创建客户端
            self.client = MongoClient(uri, **self.options)
            
            # 测试连接
            self.client.admin.command('ping')
            
            # 获取数据库
            self.db = self.client[self.database_name]
            
            if not self.silent:
                logging.info(f"MongoDB连接成功: {self.host}:{self.port}/{self.database_name}")
                
            return True
            
        except (ConnectionFailure, OperationFailure) as e:
            if not self.silent:
                logging.error(f"MongoDB连接失败: {str(e)}")
                
            self.client = None
            self.db = None
            return False
    
    def reconnect(self):
        """
        重新连接MongoDB
        
        Returns:
            bool: 是否成功重连
        """
        if self.client:
            try:
                self.client.close()
            except:
                pass
        
        return self._connect()
    
    def is_connected(self):
        """
        检查数据库连接状态
        
        Returns:
            bool: 是否已连接
        """
        if not self.client:
            return False
        
        try:
            # 尝试执行命令测试连接
            self.client.admin.command('ping')
            return True
        except:
            return False
    
    def get_client(self):
        """
        获取MongoDB客户端
        
        Returns:
            MongoClient: MongoDB客户端
        """
        if not self.is_connected():
            self.reconnect()
        
        return self.client
    
    def get_database(self, db_name=None) -> Optional[Database]:
        """
        获取数据库
        
        Args:
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            Database: MongoDB数据库对象
        """
        if db_name is None:
            db_name = self.database_name
            
        if not self.is_connected():
            self.reconnect()
            
        if not self.client:
            return None
            
        return self.client[db_name]
    
    def get_collection(self, collection_name: str, db_name=None) -> Optional[Collection]:
        """
        获取集合
        
        Args:
            collection_name: 集合名称
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            Collection: MongoDB集合对象
        """
        db = self.get_database(db_name)
        if not db:
            return None
        
        return db[collection_name]
    
    def insert_one(self, collection_name: str, document: Dict, db_name=None) -> Optional[str]:
        """
        插入单个文档
        
        Args:
            collection_name: 集合名称
            document: 文档数据
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            str: 插入的文档ID，失败返回None
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return None
        
        try:
            # 添加时间戳（如果没有的话）
            if 'created_at' not in document:
                document['created_at'] = datetime.now()
            if 'updated_at' not in document:
                document['updated_at'] = document['created_at']
            
            result = collection.insert_one(document)
            return str(result.inserted_id) if result.acknowledged else None
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB插入文档失败: {str(e)}")
            return None
    
    def insert_many(self, collection_name: str, documents: List[Dict], db_name=None, ordered=True) -> Optional[List[str]]:
        """
        插入多个文档
        
        Args:
            collection_name: 集合名称
            documents: 文档数据列表
            db_name: 数据库名称，不提供则使用默认数据库
            ordered: 是否按顺序插入
            
        Returns:
            List[str]: 插入的文档ID列表，失败返回None
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return None
        
        try:
            # 添加时间戳（如果没有的话）
            current_time = datetime.now()
            for doc in documents:
                if 'created_at' not in doc:
                    doc['created_at'] = current_time
                if 'updated_at' not in doc:
                    doc['updated_at'] = doc['created_at']
            
            result = collection.insert_many(documents, ordered=ordered)
            return [str(id) for id in result.inserted_ids] if result.acknowledged else None
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB批量插入文档失败: {str(e)}")
            return None
    
    def find_one(self, collection_name: str, filter_dict: Dict = None, projection: Dict = None, db_name=None) -> Optional[Dict]:
        """
        查询单个文档
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            projection: 投影（指定返回的字段）
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            Dict: 文档数据，未找到返回None
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return None
        
        try:
            return collection.find_one(filter_dict or {}, projection)
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB查询文档失败: {str(e)}")
            return None
    
    def find(self, collection_name: str, filter_dict: Dict = None, projection: Dict = None, 
             sort=None, limit=0, skip=0, db_name=None) -> List[Dict]:
        """
        查询多个文档
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            projection: 投影（指定返回的字段）
            sort: 排序规则，如[("field", 1)]表示按field升序
            limit: 返回的最大文档数
            skip: 跳过的文档数
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            List[Dict]: 文档数据列表
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return []
        
        try:
            cursor = collection.find(filter_dict or {}, projection)
            
            if sort:
                cursor = cursor.sort(sort)
                
            if skip:
                cursor = cursor.skip(skip)
                
            if limit:
                cursor = cursor.limit(limit)
                
            return list(cursor)
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB查询文档失败: {str(e)}")
            return []
    
    def find_to_df(self, collection_name: str, filter_dict: Dict = None, projection: Dict = None, 
                   sort=None, limit=0, skip=0, db_name=None) -> pd.DataFrame:
        """
        查询文档并转换为DataFrame
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            projection: 投影（指定返回的字段）
            sort: 排序规则，如[("field", 1)]表示按field升序
            limit: 返回的最大文档数
            skip: 跳过的文档数
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            pd.DataFrame: 文档数据DataFrame
        """
        data = self.find(collection_name, filter_dict, projection, sort, limit, skip, db_name)
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    def update_one(self, collection_name: str, filter_dict: Dict, update_dict: Dict, 
                  upsert=False, db_name=None) -> int:
        """
        更新单个文档
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            update_dict: 更新操作
            upsert: 不存在则插入
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            int: 更新的文档数量
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return 0
        
        try:
            # 确保更新操作使用$操作符
            if not any(key.startswith('$') for key in update_dict.keys()):
                update_dict = {'$set': update_dict}
                
            # 添加更新时间
            if '$set' not in update_dict:
                update_dict['$set'] = {}
            update_dict['$set']['updated_at'] = datetime.now()
            
            result = collection.update_one(filter_dict, update_dict, upsert=upsert)
            return result.modified_count + (1 if result.upserted_id else 0)
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB更新文档失败: {str(e)}")
            return 0
    
    def update_many(self, collection_name: str, filter_dict: Dict, update_dict: Dict, 
                   upsert=False, db_name=None) -> int:
        """
        更新多个文档
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            update_dict: 更新操作
            upsert: 不存在则插入
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            int: 更新的文档数量
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return 0
        
        try:
            # 确保更新操作使用$操作符
            if not any(key.startswith('$') for key in update_dict.keys()):
                update_dict = {'$set': update_dict}
                
            # 添加更新时间
            if '$set' not in update_dict:
                update_dict['$set'] = {}
            update_dict['$set']['updated_at'] = datetime.now()
            
            result = collection.update_many(filter_dict, update_dict, upsert=upsert)
            return result.modified_count + (result.upserted_id is not None)
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB批量更新文档失败: {str(e)}")
            return 0
    
    def delete_one(self, collection_name: str, filter_dict: Dict, db_name=None) -> int:
        """
        删除单个文档
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            int: 删除的文档数量
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return 0
        
        try:
            result = collection.delete_one(filter_dict)
            return result.deleted_count
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB删除文档失败: {str(e)}")
            return 0
    
    def delete_many(self, collection_name: str, filter_dict: Dict, db_name=None) -> int:
        """
        删除多个文档
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            int: 删除的文档数量
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return 0
        
        try:
            result = collection.delete_many(filter_dict)
            return result.deleted_count
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB批量删除文档失败: {str(e)}")
            return 0
    
    def count_documents(self, collection_name: str, filter_dict: Dict = None, db_name=None) -> int:
        """
        统计文档数量
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            int: 文档数量
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return 0
        
        try:
            return collection.count_documents(filter_dict or {})
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB统计文档数量失败: {str(e)}")
            return 0
    
    def distinct(self, collection_name: str, field: str, filter_dict: Dict = None, db_name=None) -> List:
        """
        获取字段不同值
        
        Args:
            collection_name: 集合名称
            field: 字段名
            filter_dict: 过滤条件
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            List: 不同值列表
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return []
        
        try:
            return collection.distinct(field, filter_dict)
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB获取不同值失败: {str(e)}")
            return []
    
    def create_index(self, collection_name: str, keys, unique=False, sparse=False, 
                     background=True, db_name=None) -> str:
        """
        创建索引
        
        Args:
            collection_name: 集合名称
            keys: 索引键，如[('field', pymongo.ASCENDING)]
            unique: 是否唯一索引
            sparse: 是否稀疏索引
            background: 是否在后台创建
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            str: 创建的索引名，失败返回空字符串
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return ""
        
        try:
            index_name = collection.create_index(
                keys,
                unique=unique,
                sparse=sparse,
                background=background
            )
            
            if not self.silent:
                logging.info(f"MongoDB创建索引成功: {collection_name}.{index_name}")
                
            return index_name
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB创建索引失败: {str(e)}")
            return ""
    
    def list_indexes(self, collection_name: str, db_name=None) -> List[Dict]:
        """
        列出集合的所有索引
        
        Args:
            collection_name: 集合名称
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            List[Dict]: 索引信息列表
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return []
        
        try:
            return list(collection.list_indexes())
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB列出索引失败: {str(e)}")
            return []
    
    def drop_index(self, collection_name: str, index_name: str, db_name=None) -> bool:
        """
        删除索引
        
        Args:
            collection_name: 集合名称
            index_name: 索引名
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            bool: 是否成功删除
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return False
        
        try:
            collection.drop_index(index_name)
            
            if not self.silent:
                logging.info(f"MongoDB删除索引成功: {collection_name}.{index_name}")
                
            return True
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB删除索引失败: {str(e)}")
            return False
    
    def aggregate(self, collection_name: str, pipeline: List[Dict], db_name=None) -> List[Dict]:
        """
        聚合查询
        
        Args:
            collection_name: 集合名称
            pipeline: 聚合管道
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            List[Dict]: 聚合结果
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return []
        
        try:
            return list(collection.aggregate(pipeline))
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB聚合查询失败: {str(e)}")
            return []
    
    def bulk_write(self, collection_name: str, operations: List, ordered=True, db_name=None) -> Dict[str, int]:
        """
        批量写操作
        
        Args:
            collection_name: 集合名称
            operations: 操作列表，如[InsertOne(), UpdateOne(), ...]
            ordered: 是否按顺序执行
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            Dict[str, int]: 操作结果统计
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return {"inserted": 0, "updated": 0, "deleted": 0}
        
        try:
            result = collection.bulk_write(operations, ordered=ordered)
            
            return {
                "inserted": result.inserted_count,
                "updated": result.modified_count,
                "deleted": result.deleted_count
            }
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB批量操作失败: {str(e)}")
            return {"inserted": 0, "updated": 0, "deleted": 0}
    
    def ensure_index(self, collection_name: str, fields: List[Tuple[str, int]], 
                     unique=False, db_name=None) -> bool:
        """
        确保索引存在
        
        Args:
            collection_name: 集合名称
            fields: 字段列表，如[("field1", 1), ("field2", -1)]
            unique: 是否唯一索引
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            bool: 是否成功
        """
        index_name = self.create_index(
            collection_name,
            fields,
            unique=unique,
            db_name=db_name
        )
        
        return bool(index_name)
    
    def drop_collection(self, collection_name: str, db_name=None) -> bool:
        """
        删除集合
        
        Args:
            collection_name: 集合名称
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            bool: 是否成功删除
        """
        db = self.get_database(db_name)
        if not db:
            return False
        
        try:
            db.drop_collection(collection_name)
            
            if not self.silent:
                logging.info(f"MongoDB删除集合成功: {collection_name}")
                
            return True
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB删除集合失败: {str(e)}")
            return False
    
    def create_collection(self, collection_name: str, db_name=None) -> bool:
        """
        创建集合
        
        Args:
            collection_name: 集合名称
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            bool: 是否成功创建
        """
        db = self.get_database(db_name)
        if not db:
            return False
        
        try:
            db.create_collection(collection_name)
            
            if not self.silent:
                logging.info(f"MongoDB创建集合成功: {collection_name}")
                
            return True
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB创建集合失败: {str(e)}")
            return False
    
    def collection_exists(self, collection_name: str, db_name=None) -> bool:
        """
        检查集合是否存在
        
        Args:
            collection_name: 集合名称
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            bool: 是否存在
        """
        db = self.get_database(db_name)
        if not db:
            return False
        
        return collection_name in db.list_collection_names()
    
    def get_collection_stats(self, collection_name: str, db_name=None) -> Dict:
        """
        获取集合统计信息
        
        Args:
            collection_name: 集合名称
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            Dict: 统计信息
        """
        db = self.get_database(db_name)
        if not db:
            return {}
        
        try:
            stats = db.command("collStats", collection_name)
            return stats
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB获取集合统计信息失败: {str(e)}")
            return {}
    
    def find_and_modify(self, collection_name: str, query: Dict, update: Dict,
                       sort=None, upsert=False, return_new=True, projection=None, db_name=None) -> Optional[Dict]:
        """
        查找并修改文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新操作
            sort: 排序规则
            upsert: 不存在则插入
            return_new: 返回修改后的文档而不是修改前
            projection: 投影（指定返回的字段）
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            Dict: 文档，未找到返回None
        """
        collection = self.get_collection(collection_name, db_name)
        if not collection:
            return None
        
        try:
            # 确保更新操作使用$操作符
            if not any(key.startswith('$') for key in update.keys()):
                update = {'$set': update}
                
            # 添加更新时间
            if '$set' not in update:
                update['$set'] = {}
            update['$set']['updated_at'] = datetime.now()
            
            result = collection.find_one_and_update(
                query,
                update,
                sort=sort,
                upsert=upsert,
                return_document=pymongo.ReturnDocument.AFTER if return_new else pymongo.ReturnDocument.BEFORE,
                projection=projection
            )
            return result
            
        except PyMongoError as e:
            if not self.silent:
                logging.error(f"MongoDB查找并修改文档失败: {str(e)}")
            return None
    
    def upsert(self, collection_name: str, filter_dict: Dict, update_dict: Dict, db_name=None) -> bool:
        """
        更新文档，不存在则插入
        
        Args:
            collection_name: 集合名称
            filter_dict: 过滤条件
            update_dict: 更新内容
            db_name: 数据库名称，不提供则使用默认数据库
            
        Returns:
            bool: 是否成功更新或插入
        """
        result = self.update_one(
            collection_name,
            filter_dict,
            update_dict,
            upsert=True,
            db_name=db_name
        )
        return result > 0
    
    def close(self):
        """
        关闭数据库连接
        """
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            
            if not self.silent:
                logging.info("MongoDB连接已关闭")
    
    def __del__(self):
        """
        析构函数
        """
        self.close()


# 创建全局MongoDB处理器实例
mongodb_handler = MongoDBHandler()