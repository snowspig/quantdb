"""
负载均衡器模块，用于在多WAN连接之间分配网络负载
"""
import os
import sys
import time
import random
import threading
import logging
import statistics
from typing import Dict, List, Any, Optional, Tuple, Set

# 导入必要的模块
from config import config_manager
from wan_manager.wan_monitor import WanMonitor


class LoadBalancer:
    """
    负载均衡器类，负责在多WAN连接之间分配网络负载
    """
    
    def __init__(self, config_path=None, silent=False):
        """
        初始化负载均衡器
        
        Args:
            config_path: 配置文件路径，不提供则使用默认配置
            silent: 静默模式，不输出日志
        """
        self.config_path = config_path
        self.silent = silent
        self.lock = threading.RLock()
        
        # 从配置中加载设置
        self.enabled = config_manager.is_wan_enabled()
        self.wan_config = config_manager.get_wan_config()
        
        # 负载均衡配置
        self.allocation_strategy = self.wan_config.get('allocation_strategy', 'round_robin')
        self.max_concurrent = self.wan_config.get('max_concurrent_connections', 20)
        
        # 创建WAN监控器
        self.wan_monitor = WanMonitor(config_path=config_path, silent=silent)
        
        # 负载统计
        self.wan_usage = {}  # {wan_idx: {'requests': int, 'active': int, 'success': int, 'fail': int, 'bytes_sent': int, 'bytes_received': int}}
        self.wan_weights = {}  # {wan_idx: float}
        self.last_wan_idx = -1
        self.rotation_counter = 0
        
        # 初始化负载统计
        self._init_usage_stats()
        
        if not self.silent and self.enabled:
            logging.info(f"负载均衡器初始化完成，策略: {self.allocation_strategy}")
    
    def _init_usage_stats(self):
        """
        初始化WAN使用统计
        """
        if not self.enabled:
            return
        
        with self.lock:
            # 获取可用的WAN接口
            available_indices = self.wan_monitor.get_available_wan_indices()
            
            # 初始化每个接口的统计数据
            for wan_idx in available_indices:
                if wan_idx not in self.wan_usage:
                    self.wan_usage[wan_idx] = {
                        'requests': 0,
                        'active': 0,
                        'success': 0,
                        'fail': 0,
                        'bytes_sent': 0,
                        'bytes_received': 0,
                        'avg_response_time': 0,
                        'last_used': 0
                    }
                    
                # 初始化权重 - 默认平均分配
                self.wan_weights[wan_idx] = 1.0
    
    def select_wan(self) -> Optional[int]:
        """
        根据负载均衡策略选择一个WAN接口
        
        Returns:
            int: 选择的WAN接口索引，无可用接口则返回None
        """
        if not self.enabled:
            return None
        
        with self.lock:
            # 获取可用的WAN接口
            available_indices = self.wan_monitor.get_available_wan_indices()
            if not available_indices:
                if not self.silent:
                    logging.warning("没有可用的WAN接口")
                return None
            
            # 更新使用统计
            self._init_usage_stats()
            
            # 根据策略选择接口
            selected_idx = None
            
            if self.allocation_strategy == 'round_robin':
                # 轮询策略
                selected_idx = self._select_round_robin(available_indices)
                
            elif self.allocation_strategy == 'random':
                # 随机策略
                selected_idx = random.choice(available_indices)
                
            elif self.allocation_strategy == 'weighted_random':
                # 加权随机策略
                selected_idx = self._select_weighted_random(available_indices)
                
            elif self.allocation_strategy == 'least_connections':
                # 最少连接策略
                selected_idx = self._select_least_connections(available_indices)
                
            elif self.allocation_strategy == 'fastest_response':
                # 最快响应策略
                selected_idx = self._select_fastest_response(available_indices)
                
            elif self.allocation_strategy == 'load_based':
                # 基于负载的策略
                selected_idx = self._select_load_based(available_indices)
                
            else:
                # 默认为轮询策略
                selected_idx = self._select_round_robin(available_indices)
            
            # 更新使用统计
            if selected_idx is not None:
                self.wan_usage[selected_idx]['requests'] += 1
                self.wan_usage[selected_idx]['active'] += 1
                self.wan_usage[selected_idx]['last_used'] = time.time()
            
            return selected_idx
    
    def report_wan_usage(self, wan_idx: int, success: bool, response_time: float, 
                        bytes_sent: int = 0, bytes_received: int = 0):
        """
        报告WAN使用情况，用于统计和负载均衡
        
        Args:
            wan_idx: WAN接口索引
            success: 请求是否成功
            response_time: 响应时间(秒)
            bytes_sent: 发送的字节数
            bytes_received: 接收的字节数
        """
        if not self.enabled or wan_idx not in self.wan_usage:
            return
        
        with self.lock:
            # 更新请求计数
            if success:
                self.wan_usage[wan_idx]['success'] += 1
            else:
                self.wan_usage[wan_idx]['fail'] += 1
            
            # 更新活动连接数
            self.wan_usage[wan_idx]['active'] = max(0, self.wan_usage[wan_idx]['active'] - 1)
            
            # 更新流量统计
            self.wan_usage[wan_idx]['bytes_sent'] += bytes_sent
            self.wan_usage[wan_idx]['bytes_received'] += bytes_received
            
            # 更新平均响应时间 (移动平均)
            if response_time > 0:
                avg = self.wan_usage[wan_idx]['avg_response_time']
                if avg == 0:
                    self.wan_usage[wan_idx]['avg_response_time'] = response_time
                else:
                    # 90%旧值 + 10%新值的移动平均
                    self.wan_usage[wan_idx]['avg_response_time'] = avg * 0.9 + response_time * 0.1
            
            # 根据成功率和响应时间更新权重
            self._update_weights()
    
    def _select_round_robin(self, available_indices: List[int]) -> int:
        """
        轮询策略选择WAN接口
        
        Args:
            available_indices: 可用WAN接口索引列表
            
        Returns:
            int: 选择的WAN接口索引
        """
        if not available_indices:
            return None
            
        # 更新轮询计数器
        self.rotation_counter += 1
        
        # 选择下一个接口
        idx = self.rotation_counter % len(available_indices)
        return available_indices[idx]
    
    def _select_weighted_random(self, available_indices: List[int]) -> int:
        """
        加权随机策略选择WAN接口
        
        Args:
            available_indices: 可用WAN接口索引列表
            
        Returns:
            int: 选择的WAN接口索引
        """
        if not available_indices:
            return None
            
        # 获取可用接口的权重
        weights = []
        for idx in available_indices:
            weights.append(self.wan_weights.get(idx, 1.0))
            
        # 如果所有权重为0，平均分配
        if sum(weights) == 0:
            weights = [1.0] * len(available_indices)
            
        # 加权随机选择
        return random.choices(available_indices, weights=weights, k=1)[0]
    
    def _select_least_connections(self, available_indices: List[int]) -> int:
        """
        最少连接策略选择WAN接口
        
        Args:
            available_indices: 可用WAN接口索引列表
            
        Returns:
            int: 选择的WAN接口索引
        """
        if not available_indices:
            return None
            
        # 找到活动连接数最少的接口
        min_active = float('inf')
        selected_idx = available_indices[0]
        
        for idx in available_indices:
            active = self.wan_usage.get(idx, {}).get('active', 0)
            if active < min_active:
                min_active = active
                selected_idx = idx
                
        return selected_idx
    
    def _select_fastest_response(self, available_indices: List[int]) -> int:
        """
        最快响应策略选择WAN接口
        
        Args:
            available_indices: 可用WAN接口索引列表
            
        Returns:
            int: 选择的WAN接口索引
        """
        if not available_indices:
            return None
            
        # 找到平均响应时间最短的接口
        min_response_time = float('inf')
        selected_idx = available_indices[0]
        
        for idx in available_indices:
            response_time = self.wan_usage.get(idx, {}).get('avg_response_time', 0)
            # 如果响应时间为0，说明没有历史数据，给一个默认的中等值
            if response_time == 0:
                response_time = 0.5
                
            if response_time < min_response_time:
                min_response_time = response_time
                selected_idx = idx
                
        return selected_idx
    
    def _select_load_based(self, available_indices: List[int]) -> int:
        """
        基于负载的策略选择WAN接口
        
        Args:
            available_indices: 可用WAN接口索引列表
            
        Returns:
            int: 选择的WAN接口索引
        """
        if not available_indices:
            return None
            
        # 计算每个接口的综合评分 (考虑活动连接数，成功率，响应时间)
        scores = {}
        for idx in available_indices:
            usage = self.wan_usage.get(idx, {})
            active = usage.get('active', 0)
            success = usage.get('success', 0)
            fail = usage.get('fail', 0)
            response_time = usage.get('avg_response_time', 0)
            
            # 计算成功率 (默认100%)
            success_rate = 1.0
            if success + fail > 0:
                success_rate = success / (success + fail)
                
            # 计算活动连接比例
            active_ratio = active / self.max_concurrent if self.max_concurrent > 0 else 0
                
            # 响应时间评分 (响应时间越短越好，最高1.0)
            response_score = 1.0
            if response_time > 0:
                # 假设1秒是一个理想的响应时间
                response_score = min(1.0, 1.0 / response_time) 
                
            # 计算综合评分 (权重可以根据需要调整)
            # 成功率权重0.4，响应时间权重0.4，连接数权重0.2
            score = (success_rate * 0.4) + (response_score * 0.4) + ((1 - active_ratio) * 0.2)
            scores[idx] = score
            
        # 选择评分最高的接口
        selected_idx = max(scores.items(), key=lambda x: x[1])[0]
        return selected_idx
    
    def _update_weights(self):
        """
        更新WAN接口权重
        """
        # 获取所有接口的统计数据
        stats = []
        for idx, usage in self.wan_usage.items():
            success = usage.get('success', 0)
            fail = usage.get('fail', 0)
            response_time = usage.get('avg_response_time', 0)
            
            # 计算成功率
            success_rate = 1.0
            if success + fail > 0:
                success_rate = success / (success + fail)
            
            # 如果成功率太低，权重降为0
            if success_rate < 0.3:  # 30%成功率阈值
                self.wan_weights[idx] = 0
                continue
                
            # 基于响应时间计算权重
            weight = 1.0
            if response_time > 0:
                # 响应时间越短权重越高，最高1.0
                weight = min(1.0, 1.0 / response_time)
                
            # 最终权重 = 成功率 * 响应时间权重
            self.wan_weights[idx] = success_rate * weight
    
    def get_wan_status(self, wan_idx: Optional[int] = None) -> Dict[str, Any]:
        """
        获取WAN状态信息
        
        Args:
            wan_idx: WAN接口索引，None表示获取所有接口
            
        Returns:
            Dict: WAN状态信息
        """
        with self.lock:
            if not self.enabled:
                return {'enabled': False}
                
            if wan_idx is not None:
                # 获取单个接口信息
                if wan_idx in self.wan_usage:
                    info = self.wan_monitor.get_wan_info(wan_idx) or {}
                    status = {
                        'usage': self.wan_usage[wan_idx],
                        'weight': self.wan_weights.get(wan_idx, 0),
                        'status': info.get('status', 'unknown'),
                        'latency': info.get('latency', -1)
                    }
                    return status
                return {}
            
            # 获取所有接口信息
            status = {
                'enabled': self.enabled,
                'strategy': self.allocation_strategy,
                'interfaces': {}
            }
            
            for idx in self.wan_usage.keys():
                info = self.wan_monitor.get_wan_info(idx) or {}
                status['interfaces'][idx] = {
                    'usage': self.wan_usage[idx],
                    'weight': self.wan_weights.get(idx, 0),
                    'status': info.get('status', 'unknown'),
                    'latency': info.get('latency', -1)
                }
                
            return status
    
    def reset_stats(self):
        """
        重置统计数据
        """
        with self.lock:
            for idx in self.wan_usage.keys():
                self.wan_usage[idx] = {
                    'requests': 0,
                    'active': 0,
                    'success': 0,
                    'fail': 0,
                    'bytes_sent': 0,
                    'bytes_received': 0,
                    'avg_response_time': 0,
                    'last_used': 0
                }
            
            # 重置权重
            for idx in self.wan_weights.keys():
                self.wan_weights[idx] = 1.0
                
            self.rotation_counter = 0
            self.last_wan_idx = -1
    
    def set_allocation_strategy(self, strategy: str) -> bool:
        """
        设置负载均衡策略
        
        Args:
            strategy: 策略名称 (round_robin, random, weighted_random, least_connections, fastest_response, load_based)
            
        Returns:
            bool: 是否设置成功
        """
        valid_strategies = [
            'round_robin', 
            'random', 
            'weighted_random', 
            'least_connections', 
            'fastest_response', 
            'load_based'
        ]
        
        if strategy not in valid_strategies:
            if not self.silent:
                logging.error(f"无效的负载均衡策略: {strategy}")
            return False
            
        with self.lock:
            self.allocation_strategy = strategy
            if not self.silent:
                logging.info(f"负载均衡策略已更改为: {strategy}")
            return True


# 创建全局负载均衡器实例
load_balancer = LoadBalancer()