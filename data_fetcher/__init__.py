"""
数据获取器包初始化文件
提供各种数据获取的功能和接口
"""
from core.base_fetcher import BaseFetcher

# 导出的组件
__all__ = [
    'BaseFetcher',
]

# 版本信息
__version__ = '0.1.0'

# 初始化日志
import logging
import os

# 确保日志目录存在
os.makedirs('logs', exist_ok=True)

# 配置根日志
logger = logging.getLogger('data_fetcher')