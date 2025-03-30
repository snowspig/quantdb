"""
MongoDB客户端模块，用于管理与MongoDB的连接和数据操作
"""
import json
import os
import yaml
from typing import Dict, Any, List, Optional, Union, Tuple
import pandas as pd
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError, ConnectionFailure, OperationFailure
from loguru import logger
import time


class MongoDBClient:
    """
    MongoDB客户端类，处理数据的存储和检索
    """
    
    def __init__(
        self, 
        uri: str = None, 
        host: str = 'localhost', 
        port: int = 27017,
        db_name: str = None,
        username: str = None, 
        password: str = None,
        auth_source: str = 'admin', 
        auth_mechanism: str = 'SCRAM-SHA-1',
        write_concern: Dict = None,
        read_preference: str = 'primaryPreferred',
        connection_pool_size: int = 100,
        timeout_ms: int = 30000,
        config_path: str = "config/config.yaml"
    ):
        """
        初始化MongoDB客户端
        
        Args:
            uri: MongoDB连接URI，如果提供则优先使用
            host: MongoDB主机地址
            port: MongoDB端口
            db_name: 数据库名称
            username: 用户名
            password: 密码
            auth_source: 认证源数据库
            auth_mechanism: 认证机制
            write_concern: 写入关注点配置
            read_preference: 读取偏好
            connection_pool_size: 连接池大小
            timeout_ms: 连接超时时间(毫秒)
            config_path: 配置文件路径，如果未提供其他参数则从此处加载
        """
        # 从配置文件加载配置（如果需要）
        self._load_config_if_needed(config_path, uri, host, port, db_name, username, 
                                   password, auth_source, auth_mechanism, write_concern,
                                   read_preference, connection_pool_size, timeout_ms)
        
        # 客户端和数据库对象
        self.client = None
        self.db = None
        self._connected = False
        
        # 记录初始化信息
        logger.info(f"MongoDB客户端初始化: host={self.host}, port={self.port}, db_name={self.db_name}, "
                    f"auth_source={self.auth_source}, auth_mechanism={self.auth_mechanism}")
    
    def _load_config_if_needed(self, config_path, uri, host, port, db_name, username, 
                             password, auth_source, auth_mechanism, write_concern,
                             read_preference, connection_pool_size, timeout_ms):
        """
        如果需要，从配置文件加载配置
        """
        # 首先保存参数传入的值
        self.uri = uri
        self.host = host
        self.port = port
        self.db_name = db_name
        self.username = username
        self.password = password
        self.auth_source = auth_source
        self.auth_mechanism = auth_mechanism
        self.write_concern = write_concern or {'w': 1}
        self.read_preference = read_preference
        self.connection_pool_size = connection_pool_size
        self.timeout_ms = timeout_ms
        
        # 如果关键参数未提供，尝试从配置文件加载
        if (not self.uri and (not self.host or not self.db_name or not self.username or not self.password)):
            try:
                if os.path.exists(config_path):
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config = yaml.safe_load(f)
                        
                    mongodb_config = config.get('mongodb', {})
                    
                    # 只在参数未提供时从配置文件加载
                    self.uri = self.uri or mongodb_config.get('uri')
                    self.host = self.host if self.host != 'localhost' else mongodb_config.get('host', 'localhost')
                    self.port = self.port if self.port != 27017 else mongodb_config.get('port', 27017)
                    self.db_name = self.db_name or mongodb_config.get('db_name')
                    self.username = self.username or mongodb_config.get('username')
                    self.password = self.password or mongodb_config.get('password')
                    self.auth_source = self.auth_source or mongodb_config.get('auth_source', 'admin')
                    self.auth_mechanism = self.auth_mechanism or mongodb_config.get('auth_mechanism', 'SCRAM-SHA-1')
                    
                    if not self.write_concern and 'write_concern' in mongodb_config:
                        self.write_concern = mongodb_config.get('write_concern')
                        
                    self.read_preference = self.read_preference or mongodb_config.get('read_preference', 'primaryPreferred')
                    self.connection_pool_size = self.connection_pool_size or mongodb_config.get('connection_pool_size', 100)
                    self.timeout_ms = self.timeout_ms or mongodb_config.get('timeout_ms', 30000)
                    
                    logger.info(f"已从配置文件 {config_path} 加载MongoDB配置")
            except Exception as e:
                logger.error(f"加载MongoDB配置文件失败: {str(e)}")
    
    def _build_connection_uri(self) -> str:
        """
        构建MongoDB连接URI
        
        Returns:
            构建好的连接URI
        """
        # 如果已经提供了URI，直接返回
        if self.uri:
            return self.uri
            
        # 导入URL转义工具
        import urllib.parse
            
        # 构建认证部分
        auth_part = ""
        if self.username and self.password:
            escaped_username = urllib.parse.quote_plus(self.username)
            escaped_password = urllib.parse.quote_plus(self.password)
            auth_part = f"{escaped_username}:{escaped_password}@"
        
        # 构建基本URI
        uri = f"mongodb://{auth_part}{self.host}:{self.port}/"
        
        # 添加参数
        params = []
        if self.auth_source:
            params.append(f"authSource={self.auth_source}")
        if self.auth_mechanism:
            params.append(f"authMechanism={self.auth_mechanism}")
            
        # 添加其他连接参数
        params.append(f"maxPoolSize={self.connection_pool_size}")
        params.append(f"connectTimeoutMS={self.timeout_ms}")
        params.append(f"socketTimeoutMS={self.timeout_ms}")
        
        # 组装最终URI
        if params:
            uri += "?" + "&".join(params)
        
        return uri
    
    def connect(self) -> bool:
        """
        连接到MongoDB服务器
        
        Returns:
            是否连接成功
        """
        if self._connected and self.client:
            logger.debug("MongoDB已连接，无需重新连接")
            return True
            
        try:
            # 构建连接URI
            uri = self._build_connection_uri()
            logger.debug(f"尝试连接到MongoDB: {uri.replace(self.password, '******') if self.password else uri}")
            
            start_time = time.time()
            
            # 创建客户端
            self.client = MongoClient(uri)
            
            # 测试连接
            self.client.admin.command('ping')
            
            # 设置数据库
            self.db = self.client[self.db_name]
            
            elapsed = time.time() - start_time
            logger.success(f"MongoDB连接成功，数据库: {self.db_name}，耗时: {elapsed:.2f}s")
            
            self._connected = True
            return True
            
        except ConnectionFailure as e:
            logger.error(f"MongoDB连接失败: 无法连接到服务器, 错误: {str(e)}, 耗时: {time.time() - start_time:.2f}s")
            self._connected = False
            return False
            
        except OperationFailure as e:
            if "Authentication failed" in str(e):
                logger.error(f"认证失败: 用户名或密码错误, 错误: {str(e)}, full error: {e.details}, 耗时: {time.time() - start_time:.2f}s")
            else:
                logger.error(f"MongoDB操作失败: {str(e)}, 耗时: {time.time() - start_time:.2f}s")
            self._connected = False
            return False
            
        except Exception as e:
            logger.error(f"MongoDB连接时发生未知错误: {str(e)}, 耗时: {time.time() - start_time:.2f}s")
            self._connected = False
            return False
    
    def is_connected(self) -> bool:
        """
        检查是否已连接到MongoDB
        
        Returns:
            是否连接状态
        """
        if not self._connected or not self.client:
            return False
            
        try:
            # 简单测试是否连接
            self.client.admin.command('ping')
            return True
        except Exception:
            self._connected = False
            return False
    
    def get_db(self, db_name: str = None) -> Database:
        """
        获取数据库引用
        
        Args:
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            MongoDB数据库对象
        
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        if not self.is_connected():
            if not self.connect():
                raise RuntimeError("未连接到MongoDB，无法获取数据库对象")
        
        db_to_use = db_name or self.db_name
        return self.client[db_to_use]
    
    def get_collection(self, collection_name: str, db_name: str = None) -> Collection:
        """
        获取集合引用
        
        Args:
            collection_name: 集合名称
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            MongoDB集合对象
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db = self.get_db(db_name)
        return db[collection_name]
    
    def insert_one(self, collection_name: str, document: Dict, db_name: str = None) -> Any:
        """
        插入单个文档
        
        Args:
            collection_name: 集合名称
            document: 要插入的文档
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            插入结果
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        return collection.insert_one(document)
    
    def insert_many(self, collection_name: str, documents: List[Dict], db_name: str = None) -> Any:
        """
        插入多个文档
        
        Args:
            collection_name: 集合名称
            documents: 要插入的文档列表
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            插入结果
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        return collection.insert_many(documents)
    
    def find_one(self, collection_name: str, query: Dict, db_name: str = None) -> Optional[Dict]:
        """
        查找单个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            查找到的文档，如果未找到则返回None
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        return collection.find_one(query)
    
    def find(self, collection_name: str, query: Dict, db_name: str = None, projection: Dict = None) -> List[Dict]:
        """
        查找多个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            db_name: 可选的数据库名称，不提供则使用默认数据库
            projection: 投影参数
            
        Returns:
            查找到的文档列表
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        return list(collection.find(query, projection))
    
    def update_one(self, collection_name: str, query: Dict, update: Dict, db_name: str = None) -> Any:
        """
        更新单个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新操作
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            更新结果
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        return collection.update_one(query, update)
    
    def update_many(self, collection_name: str, query: Dict, update: Dict, db_name: str = None) -> Any:
        """
        更新多个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新操作
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            更新结果
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        return collection.update_many(query, update)
    
    def delete_one(self, collection_name: str, query: Dict, db_name: str = None) -> Any:
        """
        删除单个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            删除结果
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        return collection.delete_one(query)
    
    def delete_many(self, collection_name: str, query: Dict, db_name: str = None) -> Any:
        """
        删除多个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            删除结果
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        return collection.delete_many(query)
    
    def ensure_indexes(self, collection_name: str, indexes: List[Dict[str, Any]], db_name: str = None):
        """
        确保集合上存在所需的索引
        
        Args:
            collection_name: 集合名称
            indexes: 索引定义列表，每个索引是包含字段和选项的字典
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        
        try:
            # 获取现有索引信息
            existing_indexes = list(collection.list_indexes())
            existing_index_names = [idx['name'] for idx in existing_indexes]
            
            logger.debug(f"集合 {collection_name} 已有 {len(existing_indexes)} 个索引")
            
            # 创建索引模型
            index_models = []
            skipped_indexes = 0
            
            for index_def in indexes:
                fields = index_def.get("fields", [])
                options = index_def.get("options", {})
                
                if not fields:
                    continue
                    
                # 将字段转换为索引键说明
                keys = []
                for field in fields:
                    if isinstance(field, str):
                        keys.append((field, ASCENDING))
                    elif isinstance(field, tuple) and len(field) == 2:
                        direction = ASCENDING if field[1] > 0 else DESCENDING
                        keys.append((field[0], direction))
                
                # 检查是否已存在相同字段的索引
                index_name = "_".join([f"{key[0]}_{key[1]}" for key in keys])
                
                # 跳过已存在的索引
                if index_name in existing_index_names:
                    logger.debug(f"索引 {index_name} 已存在于集合 {collection_name} 中，跳过创建")
                    skipped_indexes += 1
                    continue
                
                # 创建索引模型
                if keys:
                    index_models.append(IndexModel(keys, **options))
            
            # 创建新索引
            if index_models:
                try:
                    created = collection.create_indexes(index_models)
                    logger.info(f"为集合 {collection_name} 创建了 {len(created)} 个索引，跳过 {skipped_indexes} 个已存在的索引")
                except Exception as e:
                    logger.warning(f"创建索引时出现异常: {str(e)}，将尝试逐个创建索引")
                    # 如果批量创建失败，尝试单个创建
                    created_count = 0
                    for idx_model in index_models:
                        try:
                            collection.create_index(idx_model._keys, **idx_model._kwargs)
                            created_count += 1
                        except Exception as e2:
                            logger.error(f"创建单个索引失败: {str(e2)}")
                    logger.info(f"为集合 {collection_name} 逐个创建了 {created_count} 个索引，跳过 {skipped_indexes} 个已存在的索引")
            elif skipped_indexes > 0:
                logger.info(f"集合 {collection_name} 的所有 {skipped_indexes} 个索引已存在，无需创建新索引")
                
        except Exception as e:
            logger.error(f"处理索引时出现错误: {str(e)}，集合 {collection_name} 的索引可能不完整")
            import traceback
            logger.error(f"索引错误详情: {traceback.format_exc()}")
    
    def save_dataframe(
        self, 
        df: pd.DataFrame, 
        collection_name: str, 
        db_name: str = None,
        if_exists: str = 'update',
        index_fields: List[str] = None,
        chunk_size: int = 1000
    ) -> Union[int, Dict[str, int]]:
        """
        保存pandas DataFrame到指定的collection
        
        Args:
            df: pandas DataFrame对象
            collection_name: 目标collection名称
            db_name: 可选的数据库名称，不提供则使用默认数据库
            if_exists: 如果记录已存在时的处理策略, 'update'或'append'
            index_fields: 用于确定记录唯一性的字段列表
            chunk_size: 分块插入的大小，用于大数据量插入
            
        Returns:
            int或dict: 如果成功，返回新增记录数或包含详细统计的字典
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        if df.empty:
            return 0
        
        # 获取数据库名称
        db_to_use = db_name or self.db_name
        
        # 获取collection引用
        collection = self.get_collection(collection_name, db_to_use)
        
        # 首先检查并修复ts_code字段中的重复市场后缀问题
        if 'ts_code' in df.columns:
            sample_ts_code = df['ts_code'].iloc[0] if not df.empty else ''
            if '.SZ.SZ' in sample_ts_code or '.SH.SH' in sample_ts_code:
                logger.debug(f"MongoDB客户端检测到ts_code字段中的重复市场后缀: {sample_ts_code}，将进行修复")
                df['ts_code'] = df['ts_code'].str.replace('.SZ.SZ', '.SZ').str.replace('.SH.SH', '.SH')
                logger.debug(f"修复后的ts_code示例: {df['ts_code'].iloc[0] if not df.empty else ''}")
        
        # 确保trade_time字段是字符串格式
        if 'trade_time' in df.columns and not pd.api.types.is_string_dtype(df['trade_time']):
            df['trade_time'] = df['trade_time'].astype(str)
        
        # 获取总记录数
        total_records = len(df)
        
        # 将DataFrame转换为字典列表
        records = df.to_dict('records')
        
        # 确保ObjectId类型正确处理
        for record in records:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.to_pydatetime()
        
        if if_exists == 'update' and index_fields:
            # 更新模式 - 使用index_fields作为匹配条件
            new_records = 0
            updated_records = 0
            unchanged_records = 0
            
            # 分块处理，避免内存问题
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                
                for record in chunk:
                    # 构建查询条件
                    query = {field: record[field] for field in index_fields if field in record}
                    
                    if query:
                        # 检查记录是否存在
                        existing_record = collection.find_one(query)
                        if existing_record:
                            # 检查是否需要更新
                            need_update = False
                            for key, value in record.items():
                                if key == '_id':
                                    continue
                                if key not in existing_record or existing_record[key] != value:
                                    need_update = True
                                    break
                            
                            if need_update:
                                # 更新记录，但保留_id
                                record_without_id = {k: v for k, v in record.items() if k != '_id'}
                                collection.update_one(query, {"$set": record_without_id})
                                updated_records += 1
                            else:
                                unchanged_records += 1
                        else:
                            # 记录不存在，插入新记录
                            collection.insert_one(record)
                            new_records += 1
            
            # 创建索引
            if index_fields:
                self.ensure_indexes(collection_name, [{"fields": index_fields}], db_to_use)
            
            return {
                "total": total_records,
                "new": new_records,
                "updated": updated_records,
                "unchanged": unchanged_records
            }
        
        else:
            # 附加模式 - 直接插入所有记录
            # 分块插入，避免单次插入过多数据
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                if chunk:
                    collection.insert_many(chunk)
            
            # 创建索引
            if index_fields:
                self.ensure_indexes(collection_name, [{"fields": index_fields}], db_to_use)
            
            return total_records
    
    def find_latest_date(self, collection_name: str, date_field: str, db_name: str = None) -> Optional[str]:
        """
        在集合中查找最新日期
        
        Args:
            collection_name: 集合名称
            date_field: 日期字段名称
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            最新日期字符串，如果没有记录则返回None
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        
        # 查找最新日期
        result = collection.find().sort(date_field, DESCENDING).limit(1)
        docs = list(result)
        
        if docs:
            return docs[0].get(date_field)
        return None
    
    def find_stock_latest_date(self, collection_name: str, stock_code: str, date_field: str, stock_field: str = 'ts_code', db_name: str = None) -> Optional[str]:
        """
        在集合中查找特定股票的最新日期
        
        Args:
            collection_name: 集合名称
            stock_code: 股票代码
            date_field: 日期字段名称
            stock_field: 股票代码字段名称
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            最新日期字符串，如果没有记录则返回None
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        
        # 查找最新日期
        result = collection.find({stock_field: stock_code}).sort(date_field, DESCENDING).limit(1)
        docs = list(result)
        
        if docs:
            return docs[0].get(date_field)
        return None
    
    def count_documents(self, collection_name: str, query: Dict[str, Any] = None, db_name: str = None) -> int:
        """
        计算集合中的文档数量
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            db_name: 可选的数据库名称，不提供则使用默认数据库
            
        Returns:
            匹配查询的文档数量
            
        Raises:
            RuntimeError: 如果未连接到MongoDB
        """
        db_to_use = db_name or self.db_name
        collection = self.get_collection(collection_name, db_to_use)
        
        if query is None:
            query = {}
            
        return collection.count_documents(query)
    
    def close(self):
        """
        关闭MongoDB连接
        """
        if hasattr(self, 'client') and self.client:
            self.client.close()
            self._connected = False
            logger.info("MongoDB连接已关闭")


# 创建全局MongoDB客户端实例
mongodbclient = None

try:
    mongodb_client = MongoDBClient()
    logger.info("全局MongoDB客户端初始化完成")
except Exception as e:
    logger.warning(f"全局MongoDB客户端初始化失败: {str(e)}，请手动创建客户端实例")