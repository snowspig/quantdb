"""
配置模块包初始化文件
提供统一的配置读取和管理功能
"""
from config.config_manager import ConfigManager, config_manager

# 导出的组件
__all__ = [
    'ConfigManager',
    'config_manager'
]

# 版本信息
__version__ = '0.1.0'