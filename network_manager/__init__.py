"""
网络管理器包初始化文件
提供统一的网络管理、多WAN接口支持、负载均衡和会话管理功能
"""
from network_manager.network_manager import NetworkManager, network_manager

# 导出的组件
__all__ = [
    'NetworkManager',
    'network_manager'
]

# 版本信息
__version__ = '0.1.0'