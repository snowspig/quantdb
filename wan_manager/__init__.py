"""
WAN管理器包初始化文件
提供多WAN连接管理、端口分配、监控和负载均衡功能
"""
from wan_manager.wan_port_allocator import WanPortAllocator, wan_port_allocator
from wan_manager.wan_monitor import WanMonitor, wan_monitor
from wan_manager.load_balancer import LoadBalancer, load_balancer

# 导出的组件
__all__ = [
    'WanPortAllocator', 
    'WanMonitor', 
    'LoadBalancer',
    'wan_port_allocator',
    'wan_monitor',
    'load_balancer'
]

# 版本信息
__version__ = '0.1.0'