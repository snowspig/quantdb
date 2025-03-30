"""
跨WAN负载均衡模块,为API请求分配适当的WAN连接
"""
import random
import time
import threading
from typing import Dict, List, Optional, Tuple, Callable
from collections import defaultdict
from loguru import logger
from config import config_manager
from enum import Enum


class BalanceStrategy(Enum):
    """负载均衡策略枚举"""
    ROUND_ROBIN = "round_robin"  # 轮询
    RANDOM = "random"  # 随机

class LoadBalancer:
    """
    负载均衡器类,在多个WAN连接间分配任务
    """
    
    def __init__(self, strategy=BalanceStrategy.ROUND_ROBIN, silent=False):
        """
        初始化负载均衡器
        
        Args:
            strategy: 负载均衡策略,默认为轮询
            silent: 静默模式，不输出日志
        """
        # 修复嵌套配置访问
        load_balancing = config_manager.get('wan', 'load_balancing', {})
        self.method = load_balancing.get('method', 'round-robin')
        self.wan_states = {}  # WAN状态缓存
        self.wan_weights = {}  # WAN权重缓存
        self.current_index = 0  # 用于轮询
        self.request_counts = defaultdict(int)  # 每个WAN的请求计数
        self.lock = threading.Lock()
        self.wan_count = 0
        self.strategy = strategy
        self.silent = silent
        
        if not self.silent:
            logger.debug(f"负载均衡器已初始化，策略: {self.strategy.value}")
    
    def get_next_wan(self, available_wans: List[int]) -> Optional[int]:
        """
        获取下一个要使用的WAN索引
        
        Args:
            available_wans: 可用WAN索引列表
            
        Returns:
            WAN索引,如果没有可用WAN则返回None
        """
        if not available_wans:
            logger.warning("没有可用的WAN连接")
            return None
            
        with self.lock:
            # 根据负载均衡方法选择WAN
            if self.method == 'round-robin':
                return self._round_robin(available_wans)
            elif self.method == 'least-connections':
                return self._least_connections(available_wans)
            elif self.method == 'weighted':
                return self._weighted(available_wans)
            else:
                # 默认使用轮询
                return self._round_robin(available_wans)
    
    def _round_robin(self, available_wans: List[int]) -> int:
        """
        轮询算法
        
        Args:
            available_wans: 可用WAN索引列表
            
        Returns:
            选择的WAN索引
        """
        # 如果当前索引超出范围,重置为0
        if self.current_index >= len(available_wans):
            self.current_index = 0
            
        # 获取下一个WAN索引
        wan_idx = available_wans[self.current_index]
        self.current_index = (self.current_index + 1) % len(available_wans)
        
        return wan_idx
    
    def _least_connections(self, available_wans: List[int]) -> int:
        """
        最少连接算法
        
        Args:
            available_wans: 可用WAN索引列表
            
        Returns:
            选择的WAN索引
        """
        # 找出当前连接数最少的WAN
        min_connections = float('inf')
        selected_wan = None
        
        for wan_idx in available_wans:
            connections = self.request_counts[wan_idx]
            if connections < min_connections:
                min_connections = connections
                selected_wan = wan_idx
                
        return selected_wan
    
    def _weighted(self, available_wans: List[int]) -> int:
        """
        加权随机算法
        
        Args:
            available_wans: 可用WAN索引列表
            
        Returns:
            选择的WAN索引
        """
        # 简单实现,所有WAN权重相同
        return random.choice(available_wans)
    
    def increment_request_count(self, wan_idx: int):
        """
        增加WAN的请求计数
        
        Args:
            wan_idx: WAN索引
        """
        with self.lock:
            self.request_counts[wan_idx] += 1
    
    def decrement_request_count(self, wan_idx: int):
        """
        减少WAN的请求计数
        
        Args:
            wan_idx: WAN索引
        """
        with self.lock:
            if self.request_counts[wan_idx] > 0:
                self.request_counts[wan_idx] -= 1

    def set_wan_count(self, count):
        """
        设置WAN接口总数
        
        Args:
            count: WAN接口数量
        """
        self.wan_count = count
        if not self.silent:
            logger.debug(f"负载均衡器设置WAN接口数: {count}")


# 创建全局负载均衡器实例
load_balancer = LoadBalancer(silent=False) 