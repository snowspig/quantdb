"""
存储模块包初始化文件
提供数据库存储和访问功能
"""
from storage.mongodb_handler import MongoDBHandler, mongodb_handler

# 导出的组件
__all__ = [
    'MongoDBHandler',
    'mongodb_handler'
]

# 版本信息
__version__ = '0.1.0'