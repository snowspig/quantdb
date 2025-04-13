"""
WAN连接监控模块 - 检查和监控多个WAN连接的状态
"""

import time
import threading
import logging
from typing import Set
from core.config_manager import ConfigManager


class WANMonitor:
    """
    WAN连接监控器，定期检查每个WAN连接是否可用
    """
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(WANMonitor, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, silent=False):
        """
        初始化WAN监控器
        
        Args:
            silent: 静默模式，不输出日志
        """
        # 避免重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        self.logger = logging.getLogger("core.wan_manager.WANMonitor")
        
        # 获取配置 - 获取实例并使用
        _config_manager = ConfigManager() 
        monitor_config = _config_manager.get_wan_config().get('monitoring', {})
        self.check_interval = monitor_config.get('interval', 60)  # 默认60秒检查一次
        self.test_urls = monitor_config.get('test_urls', ['http://106.14.185.239:29990/test'])
        self.timeout = monitor_config.get('timeout', 10)  # 连接超时时间
        
        # 状态变量
        self.available_wans = set()
        self.status_lock = threading.Lock()
        self.monitor_thread = None
        self.should_stop = threading.Event()
        self.silent = silent
        self._initialized = True
        
        # 增加更多监控指标
        self.connection_stats = {}
        self.health_scores = {}
        self.test_methods = ['socket', 'http', 'ping']
        self.adaptive_intervals = {}  # 自适应检测间隔
        
        if not self.silent:
            self.logger.info(f"WAN监控器初始化成功,间隔: {self.check_interval}秒")
    
    def start_monitoring(self):
        """启动WAN监控线程"""
        if self.monitor_thread is not None and self.monitor_thread.is_alive():
            if not self.silent:
                self.logger.debug("WAN监控线程已在运行")
            return
            
        self.should_stop.clear()
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        if not self.silent:
            self.logger.info("WAN监控线程已启动")
    
    def stop_monitoring(self):
        """停止WAN监控线程"""
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            return
            
        self.should_stop.set()
        self.monitor_thread.join(timeout=5)
        
        if self.monitor_thread.is_alive():
            self.logger.warning("WAN监控线程未能正常停止")
        elif not self.silent:
            self.logger.info("WAN监控线程已停止")
    
    def _monitoring_loop(self):
        """WAN监控循环"""
        if not self.silent:
            self.logger.debug("WAN监控循环开始")
        
        last_check_time = 0
        
        while not self.should_stop.is_set():
            try:
                # 控制检查频率，避免太频繁检查
                current_time = time.time()
                if current_time - last_check_time > self.check_interval:
                    self._check_all_wans()
                    last_check_time = current_time
            except Exception as e:
                self.logger.error(f"WAN监控发生错误: {str(e)}")
                
            # 等待指定时间，或者直到收到停止信号
            self.should_stop.wait(min(5, self.check_interval/10))  # 等待较短时间，确保能及时响应停止信号
    
    def _check_all_wans(self):
        """检查所有WAN连接状态"""
        if not self.silent:
            self.logger.debug("开始检查所有WAN连接")
        
        # 获取WAN接口配置 - 获取实例并使用
        _config_manager = ConfigManager()
        network_config = _config_manager.get('network', {})
        wan_config = network_config.get('wan', {})
        interfaces = wan_config.get('interfaces', [])
        
        if not interfaces:
            if not self.silent: self.logger.warning("配置中未找到 network.wan.interfaces，无法检查WAN状态")
            return
            
        # 检查每个配置的接口
        available_wans = set()
        
        # 使用 enumerate 获取索引作为 wan_idx
        for wan_idx, interface in enumerate(interfaces):
            # wan_idx = interface.get('wan_idx') # 不再从字典获取
            # if wan_idx is None:
            #    self.logger.warning(f"接口 {interface.get('name')} 缺少 wan_idx")
            #    continue
                
            try:
                # 只记录结果，不输出日志
                available = self._check_wan(wan_idx)
                if available:
                    available_wans.add(wan_idx)
            except Exception as e:
                self.logger.error(f"检查WAN {wan_idx} 时发生错误: {str(e)}")
                
        # 更新可用WAN列表
        with self.status_lock:
            # 只有当列表变化时才记录日志
            if available_wans != self.available_wans:
                old_count = len(self.available_wans)
                self.available_wans = available_wans
                if not self.silent:
                    self.logger.info(f"WAN连接状态变化: {old_count} → {len(self.available_wans)} 个可用")
    
    def _check_wan(self, wan_idx: int) -> bool:
        """增强版WAN检查方法，使用多种测试方式"""
        # 记录开始时间
        start_time = time.time()
        results = []
        
        # 尝试多种测试方法
        for method in self.test_methods:
            try:
                if method == 'socket':
                    results.append(self._check_wan_socket(wan_idx))
                elif method == 'http':
                    results.append(self._check_wan_http(wan_idx))
                elif method == 'ping':
                    results.append(self._check_wan_ping(wan_idx))
            except Exception as e:
                self.logger.debug(f"WAN {wan_idx} {method}测试异常: {e}")
                results.append(False)
                
        # 更新连接统计
        elapsed = time.time() - start_time
        if wan_idx not in self.connection_stats:
            self.connection_stats[wan_idx] = {
                'total_checks': 0,
                'success_count': 0,
                'recent_times': [],
                'last_check': time.time()
            }
            
        stats = self.connection_stats[wan_idx]
        stats['total_checks'] += 1
        
        # 只要有一种方法成功即认为连接正常
        is_available = any(results)
        if is_available:
            stats['success_count'] += 1
            stats['recent_times'].append(elapsed)
            # 保留最近10次检测的时间
            if len(stats['recent_times']) > 10:
                stats['recent_times'] = stats['recent_times'][-10:]
                
        # 计算健康分数 (0-100)
        success_rate = stats['success_count'] / stats['total_checks'] if stats['total_checks'] > 0 else 0
        self.health_scores[wan_idx] = int(success_rate * 100)
        
        # 更新自适应检测间隔
        # 健康状况良好时减少检测频率，不稳定时增加检测频率
        if success_rate > 0.9:  # 90%以上成功率
            self.adaptive_intervals[wan_idx] = min(120, self.check_interval * 1.5)  # 最长2分钟
        elif success_rate < 0.5:  # 50%以下成功率
            self.adaptive_intervals[wan_idx] = max(10, self.check_interval / 2)  # 最短10秒
        else:
            self.adaptive_intervals[wan_idx] = self.check_interval
            
        return is_available
    
    def _check_wan_socket(self, wan_idx: int) -> bool:
        self.logger.debug(f"执行 WAN {wan_idx} socket 检查 (存根)")
        # TODO: 实现实际的 socket 连接检查逻辑
        return True # 暂时返回 True

    def _check_wan_http(self, wan_idx: int) -> bool:
        self.logger.debug(f"执行 WAN {wan_idx} http 检查 (存根)")
        # TODO: 实现实际的 http 请求检查逻辑
        return True # 暂时返回 True

    def _check_wan_ping(self, wan_idx: int) -> bool:
        self.logger.debug(f"执行 WAN {wan_idx} ping 检查 (存根)")
        # TODO: 实现实际的 ping 检查逻辑
        return True # 暂时返回 True
    
    def get_available_wans(self) -> Set[int]:
        """
        获取当前可用的WAN列表
        
        Returns:
            可用WAN索引集合
        """
        with self.status_lock:
            return self.available_wans.copy()
    
    def is_wan_available(self, wan_idx: int) -> bool:
        """
        检查特定WAN是否可用
        
        Args:
            wan_idx: WAN索引
            
        Returns:
            是否可用
        """
        with self.status_lock:
            return wan_idx in self.available_wans
            
    def __del__(self):
        """析构函数，确保线程停止"""
        self.stop_monitoring()


# 创建全局监控实例 - 移除，由 wan_manager/__init__.py 延迟初始化
# wan_monitor = WANMonitor(silent=False) 