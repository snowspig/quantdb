"""
WAN连接监控模块 - 检查和监控多个WAN连接的状态
"""
import os
import sys
import time
import socket
import threading
import urllib.request
import logging
from typing import Dict, List, Set
from ..config_manager import config_manager


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
        
        # 获取配置
        monitor_config = config_manager.get_wan_config().get('monitoring', {})
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
        
        # 获取WAN端口范围配置
        port_ranges = config_manager.get_wan_config().get('port_ranges', {})
        if not port_ranges:
            return
            
        # 检查每个WAN
        available_wans = set()
        already_checked = set()  # 避免重复检查
        
        for wan_idx_str in port_ranges.keys():
            try:
                wan_idx = int(wan_idx_str)
                
                # 避免重复检查
                if wan_idx in already_checked:
                    continue
                    
                already_checked.add(wan_idx)
                
                # 只记录结果，不输出日志
                available = self._check_wan(wan_idx)
                if available:
                    available_wans.add(wan_idx)
            except Exception as e:
                self.logger.error(f"检查WAN {wan_idx_str} 时发生错误: {str(e)}")
                
        # 更新可用WAN列表
        with self.status_lock:
            # 只有当列表变化时才记录日志
            if available_wans != self.available_wans:
                old_count = len(self.available_wans)
                self.available_wans = available_wans
                if not self.silent:
                    self.logger.info(f"WAN连接状态变化: {old_count} → {len(self.available_wans)} 个可用")
    
    def _check_wan(self, wan_idx: int) -> bool:
        """
        检查单个WAN连接是否可用
        
        Args:
            wan_idx: WAN索引
            
        Returns:
            是否可用
        """
        # 实际测试连接到一个test url
        try:
            from ..port_allocator import port_allocator
            
            # 获取测试URL
            test_url = self.test_urls[0] if self.test_urls else "http://106.14.185.239:29990/test"
            
            # 分配端口
            local_port = port_allocator.allocate_port(wan_idx)
            if not local_port:
                return False
                
            try:
                # 创建socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                
                # 绑定到指定端口
                sock.bind(('0.0.0.0', local_port))
                
                # 解析URL
                from urllib.parse import urlparse
                parsed_url = urlparse(test_url)
                host = parsed_url.hostname
                port = parsed_url.port or 80
                
                # 连接测试
                sock.connect((host, port))
                sock.close()
                
                return True
            except Exception:
                return False
            finally:
                # 释放端口
                port_allocator.release_port(wan_idx, local_port)
                
        except Exception:
            return False
    
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


# 创建全局监控实例
wan_monitor = WANMonitor(silent=False) 