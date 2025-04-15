#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MongoDB处理器模块
提供与MongoDB数据库的连接和交互功能
"""
import pymongo
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, OperationFailure
import yaml
import time
import logging
from urllib.parse import quote_plus


class MongoDBHandler:
    """MongoDB连接和操作处理类"""
    
    _instance = None
    
    @classmethod
    def get_instance(cls, config_path="config/config.yaml"):
        """获取单例实例"""
        if cls._instance is None:
            cls._instance = MongoDBHandler(config_path)
        return cls._instance
    
    def __init__(self, config_path="config/config.yaml"):
        """初始化MongoDB处理器"""
        self.logger = logging.getLogger("mongodb_handler")
        self.config = self.load_config(config_path)
        self.client = None
        self.db = None
        self.connected = False
        self.connection_retry_count = 0
        self.max_retry_count = 3
        self.retry_delay = 2  # 秒
        
    def load_config(self, config_path):
        """加载配置文件"""
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                config = yaml.safe_load(file)
            return config.get("mongodb", {})
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise Exception(f"Error loading configuration: {e}")

    def connect(self) -> bool:
        """连接到MongoDB数据库
        
        Returns:
            bool: 是否连接成功
        """
        # 如果已连接，直接返回
        if self.connected and self.client is not None and self.db is not None:
            try:
                # 验证连接是否仍然有效
                self.client.admin.command('ping')  
                return True
            except Exception:
                # 连接已失效，需要重新连接
                self.logger.warning("MongoDB连接已失效，将重新连接")
                self.connected = False
                if self.client:
                    try:
                        self.client.close()
                    except:
                        pass
                    self.client = None
                self.db = None
        
        # 获取配置信息
        host = self.config.get("host", "localhost")
        port = self.config.get("port", 27017)
        username = self.config.get("username", "")
        password = self.config.get("password", "")
        db_name = self.config.get("db_name", "admin")
        auth_source = self.config.get("auth_source", "admin")
        auth_mechanism = self.config.get("auth_mechanism", "DEFAULT")
        
        # 获取超时设置
        options = self.config.get("options", {})
        connect_timeout_ms = options.get("connect_timeout_ms", 10000)
        socket_timeout_ms = options.get("socket_timeout_ms", 30000)
        server_selection_timeout_ms = options.get("server_selection_timeout_ms", 30000)
        
        # 构建连接URI
        uri = "mongodb://"
        if username and password:
            uri += f"{quote_plus(username)}:{quote_plus(password)}@"
        uri += f"{host}:{port}/"
        
        # 添加参数
        params = []
        if auth_source:
            params.append(f"authSource={auth_source}")
        if auth_mechanism:
            params.append(f"authMechanism={auth_mechanism}")
        
        if params:
            uri += "?" + "&".join(params)
            
        # 记录连接尝试信息（屏蔽密码）
        safe_uri = uri
        if username and password:
            safe_uri = safe_uri.replace(quote_plus(password), "******")
        self.logger.info(f"尝试连接MongoDB: {safe_uri}")
        
        # 尝试建立连接
        start_time = time.time()
        retry_count = 0
        connected = False
        last_error = None
        
        while retry_count <= self.max_retry_count and not connected:
            try:
                # 创建连接
                self.client = pymongo.MongoClient(
                    uri,
                    connectTimeoutMS=connect_timeout_ms,
                    socketTimeoutMS=socket_timeout_ms,
                    serverSelectionTimeoutMS=server_selection_timeout_ms,
                    maxPoolSize=100,
                    minPoolSize=10,
                    retryWrites=True,
                    w=1  # 写确认级别
                )
                
                # 测试连接
                self.client.admin.command('ping')
                self.db = self.client[db_name]
                
                connected = True
                self.connected = True
                self.connection_retry_count = 0  # 重置重试计数
                
                # 连接成功日志
                elapsed = time.time() - start_time
                self.logger.info(f"MongoDB连接成功，耗时: {elapsed:.2f}秒")
                self.logger.info(f"Connected to MongoDB at {host}:{port}, database: {db_name}")
                
                return True
                
            except ServerSelectionTimeoutError as e:
                last_error = e
                retry_count += 1
                self.logger.warning(
                    f"MongoDB服务器选择超时 (尝试 {retry_count}/{self.max_retry_count}): {str(e)}"
                )
                
            except ConnectionFailure as e:
                last_error = e
                retry_count += 1
                self.logger.warning(
                    f"MongoDB连接失败 (尝试 {retry_count}/{self.max_retry_count}): {str(e)}"
                )
                
            except OperationFailure as e:
                last_error = e
                retry_count += 1
                if "Authentication failed" in str(e):
                    self.logger.error(f"MongoDB认证失败: {str(e)}")
                    # 认证失败通常不需要重试
                    break
                self.logger.warning(
                    f"MongoDB操作失败 (尝试 {retry_count}/{self.max_retry_count}): {str(e)}"
                )
                
            except Exception as e:
                last_error = e
                retry_count += 1
                self.logger.warning(
                    f"MongoDB连接异常 (尝试 {retry_count}/{self.max_retry_count}): {str(e)}"
                )
            
            # 重试前等待
            if retry_count <= self.max_retry_count:
                time.sleep(self.retry_delay * retry_count)  # 指数退避
        
        # 连接失败
        self.connected = False
        self.client = None
        self.db = None
        self.connection_retry_count += 1
        elapsed = time.time() - start_time
        
        # 记录失败详情
        if last_error:
            self.logger.error(
                f"MongoDB连接失败，耗时: {elapsed:.2f}秒，错误: {str(last_error)}, "
                f"已重试 {self.connection_retry_count} 次"
            )
        else:
            self.logger.error(
                f"MongoDB连接失败，耗时: {elapsed:.2f}秒，未知错误，"
                f"已重试 {self.connection_retry_count} 次"
            )
        
        return False

    def is_connected(self) -> bool:
        """检查是否已成功连接
        
        Returns:
            bool: 是否连接状态
        """
        if not self.connected or self.client is None or self.db is None:
            return False
        
        try:
            # 发送ping命令验证连接
            self.client.admin.command('ping')
            return True
        except Exception as e:
            self.logger.warning(f"MongoDB连接检查失败: {str(e)}")
            self.connected = False
            return False
    
    def get_database(self):
        """获取数据库对象
        
        Returns:
            pymongo.database.Database: MongoDB数据库对象
            
        Raises:
            Exception: 如果连接失败
        """
        if not self.is_connected():
            if not self.connect():
                raise Exception("无法连接到MongoDB数据库")
        return self.db
    
    def get_collection(self, collection_name):
        """获取集合对象
        
        Args:
            collection_name: 集合名称
            
        Returns:
            pymongo.collection.Collection: MongoDB集合对象
            
        Raises:
            Exception: 如果连接失败
        """
        db = self.get_database()
        return db[collection_name]

    def insert_document(self, collection_name, document):
        """插入文档
        
        Args:
            collection_name: 集合名称
            document: 要插入的文档
            
        Returns:
            pymongo.results.InsertOneResult: 插入结果
            
        Raises:
            Exception: 如果插入失败
        """
        try:
            collection = self.get_collection(collection_name)
            result = collection.insert_one(document)
            return result.inserted_id
        except Exception as e:
            self.logger.error(f"插入文档失败: {str(e)}")
            raise Exception(f"插入文档失败: {str(e)}")

    def insert_many_documents(self, collection_name, documents, ordered=False):
        """批量插入文档
        
        Args:
            collection_name: 集合名称
            documents: 要插入的文档列表
            ordered: 是否按顺序执行（默认False，提高性能）
            
        Returns:
            pymongo.results.InsertManyResult: 插入结果
            
        Raises:
            pymongo.errors.BulkWriteError: 如果批量写入过程中有错误
            Exception: 如果有其他插入失败
        """
        try:
            collection = self.get_collection(collection_name)
            result = collection.insert_many(documents, ordered=ordered)
            return result.inserted_ids
        except pymongo.errors.BulkWriteError as e:
            # 保留原始的BulkWriteError异常，添加日志
            self.logger.warning(f"批量插入文档过程中发生错误: {str(e)}")
            
            # 判断是否包含重复键错误
            if "E11000 duplicate key error" in str(e):
                write_errors = e.details.get('writeErrors', [])
                duplicate_errors = sum(1 for err in write_errors if err.get('code') == 11000)
                self.logger.info(f"包含{duplicate_errors}个重复键错误，非致命错误")
                
                # 记录一些错误样例
                if duplicate_errors > 0:
                    sample_errors = [err.get('errmsg', '') for err in write_errors[:3] if err.get('code') == 11000]
                    self.logger.debug(f"重复键错误示例: {sample_errors}")
            
            # 直接抛出原始异常，不重新包装
            raise
        except Exception as e:
            self.logger.error(f"批量插入文档失败: {str(e)}")
            raise

    def find_document(self, collection_name, query):
        """查找单个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            
        Returns:
            dict: 查找的文档或None
            
        Raises:
            Exception: 如果查询失败
        """
        try:
            collection = self.get_collection(collection_name)
            return collection.find_one(query)
        except Exception as e:
            self.logger.error(f"查找文档失败: {str(e)}")
            raise Exception(f"查找文档失败: {str(e)}")

    def find_documents(self, collection_name, query=None):
        """查找多个文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件，默认为None（查找所有）
            
        Returns:
            list: 文档列表
            
        Raises:
            Exception: 如果查询失败
        """
        try:
            collection = self.get_collection(collection_name)
            return list(collection.find(query or {}))
        except Exception as e:
            self.logger.error(f"查找多个文档失败: {str(e)}")
            raise Exception(f"查找多个文档失败: {str(e)}")

    def update_document(self, collection_name, query, update):
        """更新文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新内容
            
        Returns:
            pymongo.results.UpdateResult: 更新结果
            
        Raises:
            Exception: 如果更新失败
        """
        try:
            collection = self.get_collection(collection_name)
            # 确保更新操作符正确
            if not any(k.startswith('$') for k in update.keys()):
                update = {"$set": update}
            result = collection.update_one(query, update)
            return result.modified_count
        except Exception as e:
            self.logger.error(f"更新文档失败: {str(e)}")
            raise Exception(f"更新文档失败: {str(e)}")

    def update_many_documents(self, collection_name, query, update):
        """批量更新文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            update: 更新内容
            
        Returns:
            pymongo.results.UpdateResult: 更新结果
            
        Raises:
            Exception: 如果更新失败
        """
        try:
            collection = self.get_collection(collection_name)
            # 确保更新操作符正确
            if not any(k.startswith('$') for k in update.keys()):
                update = {"$set": update}
            result = collection.update_many(query, update)
            return result.modified_count
        except Exception as e:
            self.logger.error(f"批量更新文档失败: {str(e)}")
            raise Exception(f"批量更新文档失败: {str(e)}")

    def delete_document(self, collection_name, query):
        """删除文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            
        Returns:
            pymongo.results.DeleteResult: 删除结果
            
        Raises:
            Exception: 如果删除失败
        """
        try:
            collection = self.get_collection(collection_name)
            result = collection.delete_one(query)
            return result.deleted_count
        except Exception as e:
            self.logger.error(f"删除文档失败: {str(e)}")
            raise Exception(f"删除文档失败: {str(e)}")

    def delete_many_documents(self, collection_name, query):
        """批量删除文档
        
        Args:
            collection_name: 集合名称
            query: 查询条件
            
        Returns:
            pymongo.results.DeleteResult: 删除结果
            
        Raises:
            Exception: 如果删除失败
        """
        try:
            collection = self.get_collection(collection_name)
            result = collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            self.logger.error(f"批量删除文档失败: {str(e)}")
            raise Exception(f"批量删除文档失败: {str(e)}")

    def count_documents(self, collection_name, query=None):
        """统计文档数量
        
        Args:
            collection_name: 集合名称
            query: 查询条件，默认为None（统计所有）
            
        Returns:
            int: 文档数量
            
        Raises:
            Exception: 如果统计失败
        """
        try:
            collection = self.get_collection(collection_name)
            return collection.count_documents(query or {})
        except Exception as e:
            self.logger.error(f"统计文档数量失败: {str(e)}")
            raise Exception(f"统计文档数量失败: {str(e)}")

    def create_index(self, collection_name, keys, **kwargs):
        """创建索引
        
        Args:
            collection_name: 集合名称
            keys: 索引键，可以是字符串或(key, direction)元组列表
            **kwargs: 其他索引选项
            
        Returns:
            str: 索引名称
            
        Raises:
            Exception: 如果创建索引失败
        """
        try:
            collection = self.get_collection(collection_name)
            return collection.create_index(keys, **kwargs)
        except Exception as e:
            self.logger.error(f"创建索引失败: {str(e)}")
            raise Exception(f"创建索引失败: {str(e)}")

    def list_indexes(self, collection_name):
        """列出集合的所有索引
        
        Args:
            collection_name: 集合名称
            
        Returns:
            list: 索引列表
            
        Raises:
            Exception: 如果获取索引失败
        """
        try:
            collection = self.get_collection(collection_name)
            return list(collection.list_indexes())
        except Exception as e:
            self.logger.error(f"获取索引列表失败: {str(e)}")
            raise Exception(f"获取索引列表失败: {str(e)}")

    def get_collection_indexes(self, collection_name):
        """获取集合的所有索引
        
        Args:
            collection_name: 集合名称
            
        Returns:
            list: 索引列表
            
        Raises:
            Exception: 如果获取索引失败
        """
        try:
            return self.list_indexes(collection_name)
        except Exception as e:
            self.logger.error(f"获取集合索引失败: {str(e)}")
            raise Exception(f"获取集合索引失败: {str(e)}")

    def drop_index(self, collection_name, index_name):
        """删除索引
        
        Args:
            collection_name: 集合名称
            index_name: 索引名称
            
        Raises:
            Exception: 如果删除索引失败
        """
        try:
            collection = self.get_collection(collection_name)
            collection.drop_index(index_name)
        except Exception as e:
            self.logger.error(f"删除索引失败: {str(e)}")
            raise Exception(f"删除索引失败: {str(e)}")

    def close_connection(self):
        """关闭MongoDB连接"""
        if self.client:
            try:
                self.client.close()
                self.logger.info("MongoDB连接已关闭")
            except Exception as e:
                self.logger.warning(f"关闭MongoDB连接时出错: {str(e)}")
            finally:
                self.client = None
                self.db = None
                self.connected = False

    def bulk_write(self, collection_name, operations, ordered=False):
        """执行批量写入操作
        
        Args:
            collection_name: 集合名称
            operations: 操作列表（如UpdateOne, InsertOne等）
            ordered: 是否按顺序执行（默认False，提高性能）
            
        Returns:
            pymongo.results.BulkWriteResult: 批量写入结果
            
        Raises:
            pymongo.errors.BulkWriteError: 如果批量写入过程中有错误
            Exception: 如果批量写入失败
        """
        try:
            collection = self.get_collection(collection_name)
            result = collection.bulk_write(operations, ordered=ordered)
            return result
        except pymongo.errors.BulkWriteError as e:
            # 保留原始的BulkWriteError异常，添加日志
            self.logger.warning(f"批量写入操作过程中发生错误: {str(e)}")
            
            # 判断是否包含重复键错误
            if "E11000 duplicate key error" in str(e):
                write_errors = e.details.get('writeErrors', [])
                duplicate_errors = sum(1 for err in write_errors if err.get('code') == 11000)
                self.logger.info(f"包含{duplicate_errors}个重复键错误，非致命错误")
                
                # 记录一些错误样例
                if duplicate_errors > 0:
                    sample_errors = [err.get('errmsg', '') for err in write_errors[:3] if err.get('code') == 11000]
                    self.logger.debug(f"重复键错误示例: {sample_errors}")
            
            # 直接抛出原始异常，不重新包装
            raise
        except Exception as e:
            self.logger.error(f"批量写入操作失败: {str(e)}")
            raise
            
    def create_collection(self, collection_name, **options):
        """创建集合
        
        Args:
            collection_name: 集合名称
            **options: 集合选项
            
        Returns:
            pymongo.collection.Collection: 创建的集合
            
        Raises:
            Exception: 如果创建集合失败
        """
        try:
            db = self.get_database()
            if collection_name in db.list_collection_names():
                return db[collection_name]
            return db.create_collection(collection_name, **options)
        except Exception as e:
            self.logger.error(f"创建集合失败: {str(e)}")
            raise Exception(f"创建集合失败: {str(e)}")
            
    def collection_exists(self, collection_name):
        """检查集合是否存在
        
        Args:
            collection_name: 集合名称
            
        Returns:
            bool: 是否存在
        """
        try:
            db = self.get_database()
            return collection_name in db.list_collection_names()
        except Exception as e:
            self.logger.error(f"检查集合是否存在失败: {str(e)}")
            return False
            
    def create_standard_indexes(self, collection_name, field_mappings):
        """为集合创建标准索引
        
        Args:
            collection_name: 集合名称
            field_mappings: 字段映射字典，格式为 {字段名: 索引类型}
                索引类型可以为: 'date', 'code', 'number'等
                
        Returns:
            list: 创建的索引名称列表
        """
        try:
            collection = self.get_collection(collection_name)
            indexes = []
            
            # 创建日期索引
            date_fields = [field for field, type_ in field_mappings.items() if type_ == 'date']
            if date_fields:
                for field in date_fields:
                    index_name = f"{field}_idx"
                    collection.create_index([(field, 1)], background=True, name=index_name)
                    indexes.append(index_name)
                    
            # 创建代码索引
            code_fields = [field for field, type_ in field_mappings.items() if type_ == 'code']
            if code_fields:
                for field in code_fields:
                    index_name = f"{field}_idx"
                    collection.create_index([(field, 1)], background=True, name=index_name)
                    indexes.append(index_name)
                    
            # 创建数值索引
            number_fields = [field for field, type_ in field_mappings.items() if type_ == 'number']
            if number_fields:
                for field in number_fields:
                    index_name = f"{field}_idx"
                    collection.create_index([(field, 1)], background=True, name=index_name)
                    indexes.append(index_name)
                    
            # 如果有多个代码/日期字段，创建复合索引
            if len(code_fields) >= 1 and len(date_fields) >= 1:
                # 创建(code, date)复合索引
                code_field = code_fields[0]
                date_field = date_fields[0]
                index_name = f"{code_field}_{date_field}_idx"
                collection.create_index(
                    [(code_field, 1), (date_field, 1)], 
                    background=True, 
                    name=index_name
                )
                indexes.append(index_name)
                
            return indexes
        except Exception as e:
            self.logger.error(f"创建标准索引失败: {str(e)}")
            return []


# 全局单例实例
mongodb_handler = None


def init_mongodb_handler(config_path="config/config.yaml"):
    """初始化MongoDB处理器全局实例
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        MongoDBHandler: MongoDB处理器实例
    """
    global mongodb_handler
    if mongodb_handler is None:
        mongodb_handler = MongoDBHandler(config_path)
        # 尝试立即连接
        try:
            mongodb_handler.connect()
        except Exception as e:
            logging.getLogger("mongodb").warning(f"初始化时连接MongoDB失败: {str(e)}")
    return mongodb_handler


# 自动初始化全局实例 - 移除或注释掉，避免在导入时自动连接
# init_mongodb_handler()
