"""
跨WAN负载均衡模块,为API请求分配适当的WAN连接
"""
import random
import time
import threading
from typing import Dict, List, Optional, Tuple, Callable
from collections import defaultdict
import logging
from enum import Enum
from core.config_manager import ConfigManager


class BalanceStrategy(Enum):
    """负载均衡策略枚举"""
    ROUND_ROBIN = "round_robin"  # 轮询
    RANDOM = "random"  # 随机
    WEIGHTED = "weighted"  # 加权轮询


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
        self.logger = logging.getLogger("core.wan_manager.LoadBalancer")
        self.silent = silent
        
        # 从配置中获取负载均衡设置 - 获取实例并使用
        _config_manager = ConfigManager()
        load_balancing = _config_manager.get_wan_config().get('load_balancing', {})
        self.method = load_balancing.get('method', 'round_robin')
        
        # 初始化状态和统计信息
        self.wan_states = {}  # WAN状态缓存
        self.wan_weights = load_balancing.get('weights', {})  # WAN权重缓存
        self.current_index = 0  # 用于轮询
        self.request_counts = defaultdict(int)  # 每个WAN的请求计数
        self.lock = threading.Lock()
        self.available_wans = []
        self.strategy = strategy
        
        # 接口性能统计
        self.wan_stats = {}
        
        if not self.silent:
            self.logger.debug(f"负载均衡器已初始化，策略: {self.method}")
    
    def get_next_wan(self) -> Optional[int]:
        """
        获取下一个要使用的WAN索引
        
        Returns:
            WAN索引,如果没有可用WAN则返回None
        """
        if not self.available_wans:
            self.logger.warning("没有可用的WAN连接")
            return None
            
        with self.lock:
            # 根据负载均衡方法选择WAN
            if self.method == 'round_robin':
                return self._round_robin()
            elif self.method == 'least_connections':
                return self._least_connections()
            elif self.method == 'weighted':
                return self._weighted()
            else:
                # 默认使用轮询
                return self._round_robin()
    
    def _round_robin(self) -> int:
        """
        轮询算法
        
        Returns:
            选择的WAN索引
        """
        # 如果当前索引超出范围,重置为0
        if self.current_index >= len(self.available_wans):
            self.current_index = 0
            
        # 获取下一个WAN索引
        wan_idx = self.available_wans[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.available_wans)
        
        return wan_idx
    
    def _least_connections(self) -> int:
        """
        最少连接算法
        
        Returns:
            选择的WAN索引
        """
        # 找出当前连接数最少的WAN
        min_connections = float('inf')
        selected_wan = None
        
        for wan_idx in self.available_wans:
            connections = self.request_counts[wan_idx]
            if connections < min_connections:
                min_connections = connections
                selected_wan = wan_idx
                
        return selected_wan if selected_wan is not None else self.available_wans[0]
    
    def _weighted(self) -> int:
        """
        加权随机算法
        
        Returns:
            选择的WAN索引
        """
        # 获取所有可用WAN的权重
        total_weight = 0
        wan_weights = []
        
        for wan_idx in self.available_wans:
            weight = self.wan_weights.get(str(wan_idx), 1)  # 默认权重为1
            wan_weights.append((wan_idx, weight))
            total_weight += weight
            
        if total_weight == 0:
            return self._round_robin()
            
        # 随机选择
        r = random.uniform(0, total_weight)
        upto = 0
        
        for wan_idx, weight in wan_weights:
            upto += weight
            if upto >= r:
                return wan_idx
                
        # 默认情况
        return self.available_wans[0]
    
    def update_available_wans(self, available_wans: List[int]):
        """
        更新可用WAN接口列表
        
        Args:
            available_wans: 可用WAN接口索引列表
        """
        with self.lock:
            # 只有当列表变化时才更新和记录日志
            if set(self.available_wans) != set(available_wans):
                self.available_wans = available_wans
                self.current_index = 0
                
                # 初始化统计信息
                for wan_idx in self.available_wans:
                    if wan_idx not in self.wan_stats:
                        self.wan_stats[wan_idx] = {
                            'success_count': 0,
                            'error_count': 0,
                            'total_requests': 0,
                            'last_success_time': 0,
                            'last_error_time': 0,
                            'avg_response_time': 0
                        }
                
                if not self.silent:
                    self.logger.debug(f"负载均衡器更新可用WAN列表: {self.available_wans}")
                
    def select_wan(self) -> int:
        """
        选择一个WAN接口用于下一个请求
        
        Returns:
            int: 选中的WAN接口索引，如果没有可用接口则返回-1
        """
        wan_idx = self.get_next_wan()
        if wan_idx is None:
            self.logger.warning("没有可用的WAN接口")
            return -1
        return wan_idx
    
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
                
    def record_success(self, wan_idx: int, response_time: float = 0):
        """
        记录请求成功
        
        Args:
            wan_idx: WAN接口索引
            response_time: 响应时间（秒）
        """
        with self.lock:
            if wan_idx in self.wan_stats:
                stats = self.wan_stats[wan_idx]
                stats['success_count'] += 1
                stats['total_requests'] += 1
                stats['last_success_time'] = time.time()
                self.increment_request_count(wan_idx)
                
                # 更新平均响应时间
                if response_time > 0:
                    if stats['avg_response_time'] == 0:
                        stats['avg_response_time'] = response_time
                    else:
                        stats['avg_response_time'] = (stats['avg_response_time'] * (stats['success_count'] - 1) + response_time) / stats['success_count']
                        
    def record_error(self, wan_idx: int):
        """
        记录请求失败
        
        Args:
            wan_idx: WAN接口索引
        """
        with self.lock:
            if wan_idx in self.wan_stats:
                stats = self.wan_stats[wan_idx]
                stats['error_count'] += 1
                stats['total_requests'] += 1
                stats['last_error_time'] = time.time()

    def get_stats(self) -> Dict:
        """
        获取所有WAN接口的统计信息
        
        Returns:
            Dict: 统计信息
        """
        with self.lock:
            return {
                'method': self.method,
                'available_wans': self.available_wans,
                'request_counts': dict(self.request_counts),
                'stats': self.wan_stats
            }

    def initialize(self):
        """初始化负载均衡器状态"""
        # 添加更多状态跟踪
        self.response_times = {wan_idx: [] for wan_idx in self.available_wans}
        self.failure_counts = {wan_idx: 0 for wan_idx in self.available_wans}
        self.circuit_broken = {wan_idx: False for wan_idx in self.available_wans}
        self.last_check_time = {wan_idx: 0 for wan_idx in self.available_wans}
        self.method = 'adaptive'  # 使用自适应策略
        
    def _adaptive_selection(self) -> int:
        """自适应负载均衡策略"""
        # 计算每个WAN的得分 (响应时间越短，成功率越高，得分越高)
        scores = {}
        current_time = time.time()
        
        for wan_idx in self.available_wans:
            # 检查是否处于熔断状态
            if self.circuit_broken[wan_idx]:
                # 定期尝试恢复熔断的连接
                if current_time - self.last_check_time[wan_idx] > 60:  # 1分钟恢复检查
                    self.circuit_broken[wan_idx] = False
                else:
                    scores[wan_idx] = 0
                    continue
                
            # 计算最近的响应时间平均值
            avg_time = 1.0  # 默认值
            if self.response_times[wan_idx]:
                # 只使用最近的5个响应时间
                recent_times = self.response_times[wan_idx][-5:]
                if recent_times:
                    avg_time = sum(recent_times) / len(recent_times)
                
            # 计算成功率
            total = self.wan_stats[wan_idx]['success_count'] + self.wan_stats[wan_idx]['error_count']
            success_rate = 0.5  # 默认值
            if total > 0:
                success_rate = self.wan_stats[wan_idx]['success_count'] / total
            
            # 综合评分，响应时间反比，成功率正比
            scores[wan_idx] = (success_rate * 0.7) + (1.0 / (avg_time + 0.1)) * 0.3
        
        # 选择得分最高的WAN
        if not scores:
            return self._round_robin()  # 回退到轮询
        
        best_wan = max(scores.items(), key=lambda x: x[1])[0]
        return best_wan


# 创建全局负载均衡器实例 - 移除，由 wan_manager/__init__.py 延迟初始化
# load_balancer = LoadBalancer(silent=False) 