"""
WAN连接监控模块 - 检查和监控多个WAN连接的状态
"""
import os
import sys
import time
import socket
import threading
import urllib.request
from typing import Dict, List, Set
from loguru import logger
from config import config_manager


class WANMonitor:
    """
    WAN连接监控器，定期检查每个WAN连接是否可用
    """
    
    def __init__(self, silent=False):
        """
        初始化WAN监控器
        
        Args:
            silent: 静默模式，不输出日志
        """
        # 获取配置
        monitor_config = config_manager.get('wan', 'monitoring', {})
        self.check_interval = monitor_config.get('interval', 60)  # 默认60秒检查一次
        self.test_urls = monitor_config.get('test_urls', ['http://www.baidu.com', 'http://www.qq.com'])
        self.timeout = monitor_config.get('timeout', 10)  # 连接超时时间
        
        # 状态变量
        self.available_wans = set()
        self.status_lock = threading.Lock()
        self.monitor_thread = None
        self.should_stop = threading.Event()
        self.silent = silent
        
        if not self.silent:
            logger.info(f"WAN监控器初始化成功,间隔: {self.check_interval}秒")
    
    def start_monitoring(self):
        """启动WAN监控线程"""
        if self.monitor_thread is not None and self.monitor_thread.is_alive():
            if not self.silent:
                logger.warning("WAN监控线程已在运行")
            return
            
        self.should_stop.clear()
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        if not self.silent:
            logger.info("WAN监控线程已启动")
    
    def stop_monitoring(self):
        """停止WAN监控线程"""
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            return
            
        self.should_stop.set()
        self.monitor_thread.join(timeout=5)
        
        if self.monitor_thread.is_alive():
            logger.warning("WAN监控线程未能正常停止")
        elif not self.silent:
            logger.info("WAN监控线程已停止")
    
    def _monitoring_loop(self):
        """WAN监控循环"""
        if not self.silent:
            logger.debug("WAN监控循环开始")
        
        while not self.should_stop.is_set():
            try:
                self._check_all_wans()
            except Exception as e:
                logger.error(f"WAN监控发生错误: {str(e)}")
                
            # 等待指定时间，或者直到收到停止信号
            self.should_stop.wait(self.check_interval)
    
    def _check_all_wans(self):
        """检查所有WAN连接状态"""
        if not self.silent:
            logger.debug("开始检查所有WAN连接")
        
        # 获取WAN端口范围配置
        port_ranges = config_manager.get('wan', 'port_ranges', {})
        if not port_ranges:
            logger.warning("未找到WAN端口范围配置")
            return
            
        # 检查每个WAN
        available_wans = set()
        for wan_idx in port_ranges.keys():
            try:
                wan_idx = int(wan_idx)
                available = self._check_wan(wan_idx)
                if available:
                    available_wans.add(wan_idx)
            except Exception as e:
                logger.error(f"检查WAN {wan_idx} 时发生错误: {str(e)}")
                
        # 更新可用WAN列表
        with self.status_lock:
            self.available_wans = available_wans
            
        if not self.silent:
            logger.info(f"WAN连接检查完成,可用连接: {len(self.available_wans)}")
    
    def _check_wan(self, wan_idx: int) -> bool:
        """
        检查单个WAN连接是否可用
        
        Args:
            wan_idx: WAN索引
            
        Returns:
            是否可用
        """
        # 为简化实现，这里直接返回True
        # 实际应用中应该实现真正的连接检查
        # 例如尝试通过特定WAN发送HTTP请求
        return True
    
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


# 创建全局监控实例
wan_monitor = WANMonitor(silent=False) 