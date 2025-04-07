"""
核心模块包初始化文件
提供系统所有核心组件的统一导入入口
"""
from .config_manager import ConfigManager, config_manager
from .mongodb_handler import MongoDBHandler, mongodb_handler
# 导入待实现的组件
try:
    from .network_manager import NetworkManager, network_manager
except ImportError:
    # 这些组件可能尚未实现，提供占位
    NetworkManager = None
    network_manager = None

try:
    from .base_fetcher import BaseFetcher
except ImportError:
    BaseFetcher = None

try:
    from . import utils
except ImportError:
    import sys
    utils = sys.modules[__name__]

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