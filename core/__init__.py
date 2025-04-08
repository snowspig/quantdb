#!/usr/bin/env python
"""
量化平台核心模块
包含公共组件和服务
"""

# 导出主要的类和模块
from core.tushare_client_wan import TushareClientWAN, SourceAddressAdapter

"""
核心模块包初始化文件
提供系统所有核心组件的统一导入入口
"""
from .config_manager import ConfigManager, config_manager
from .mongodb_handler import MongoDBHandler, mongodb_handler

# 导入其他组件但不创建实例
try:
    from .base_fetcher import BaseFetcher
except ImportError:
    BaseFetcher = None

try:
    from . import utils
except ImportError:
    import sys
    utils = sys.modules[__name__]

# 最后导入网络管理器及其单例实例
try:
    from .network_manager import NetworkManager, network_manager
except ImportError:
    NetworkManager = None
    network_manager = None

# 导出的组件
__all__ = [
    'ConfigManager',
    'config_manager',
    'MongoDBHandler',
    'mongodb_handler',
    'NetworkManager',
    'network_manager',
    'BaseFetcher',
    'utils'
]

# 版本信息
__version__ = '0.1.0'