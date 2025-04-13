#!/usr/bin/env python
"""
量化平台核心模块
包含公共组件和服务
"""

"""
核心模块包初始化文件
提供系统所有核心组件的统一导入入口
"""
# 配置管理模块
from .config_manager import ConfigManager
# 数据库模块
from .mongodb_handler import MongoDBHandler, mongodb_handler

# 网络和WAN管理模块
from .tushare_client_wan import TushareClientWAN, SourceAddressAdapter

# 公共工具类
from . import utils

# WAN管理模块导出
from .wan_manager import (
    get_port_allocator,
    get_wan_port_pool,
    get_load_balancer,
    is_windows,
    generate_optimization_suggestions
)

# 基础类
try:
    from .base_fetcher import BaseFetcher
except ImportError:
    BaseFetcher = None



# 导出的组件
__all__ = [
    # 基础组件
    'ConfigManager',
    'MongoDBHandler',
    'mongodb_handler',
    'utils',
    'BaseFetcher',
    
    # Tushare相关
    'TushareClientWAN',
    'SourceAddressAdapter',
    
    # WAN管理相关
    'get_port_allocator',
    'get_wan_port_pool',
    'get_load_balancer',
    'is_windows',
    'generate_optimization_suggestions'
]

# 版本信息
__version__ = '0.2.0'