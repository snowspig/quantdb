"""
WAN监控器模块，负责监控WAN接口状态并提供实时状态更新
"""
import os
import sys
import time
import socket
import threading
import subprocess
import platform
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set

# 导入必要的模块
from config import config_manager


class WanMonitor:
    """
    WAN监控器类，负责监控多WAN接口状态
    """
    
    def __init__(self, config_path=None, silent=False):
        """
        初始化WAN监控器
        
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
        
        # 监控配置
        self.monitor_interval = self.wan_config.get('monitor_interval', 60)  # 秒
        self.ping_timeout = self.wan_config.get('connection_timeout', 30)  # 秒
        
        # 接口配置
        self.interfaces = self.wan_config.get('interfaces', [])
        
        # 接口状态
        self.wan_status = {}  # {wan_idx: {'status': 'up|down', 'latency': float, 'last_check': timestamp, ...}}
        
        # 测试目标
        self.test_targets = [
            "8.8.8.8",       # Google DNS
            "1.1.1.1",       # Cloudflare DNS
            "114.114.114.114"  # China public DNS
        ]
        
        # 初始化状态
        self._init_status()
        
        # 启动监控线程（如果启用）
        self._monitor_thread = None
        self._stop_event = threading.Event()
        if self.enabled:
            self._start_monitoring()
    
    def _init_status(self):
        """
        初始化接口状态
        """
        if not self.enabled:
            return
        
        with self.lock:
            for i, interface in enumerate(self.interfaces):
                if interface.get('enabled', True):
                    self.wan_status[i] = {
                        'ip': interface.get('ip', 'unknown'),
                        'name': interface.get('name', f'wan{i}'),
                        'interface': interface.get('interface', f'eth{i}'),
                        'status': 'unknown',
                        'latency': -1,
                        'last_check': 0,
                        'up_since': 0,
                        'down_since': 0,
                        'check_count': 0,
                        'success_count': 0,
                        'fail_count': 0
                    }
                    
                    if not self.silent:
                        logging.debug(f"已初始化WAN接口 {i}: {interface.get('name', f'wan{i}')}")
    
    def _start_monitoring(self):
        """
        启动监控线程
        """
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            return
        
        if not self.enabled:
            return
            
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, name="WanMonitor")
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
        
        if not self.silent:
            logging.info(f"WAN监控器已启动，监控间隔: {self.monitor_interval}秒")
    
    def _stop_monitoring(self):
        """
        停止监控线程
        """
        if self._monitor_thread is not None and self._monitor_thread.is_alive():
            self._stop_event.set()
            self._monitor_thread.join(timeout=5)
            if not self.silent:
                logging.info("WAN监控器已停止")
    
    def _monitor_loop(self):
        """
        监控循环
        """
        if not self.silent:
            logging.info("WAN监控线程已启动")
            
        # 立即进行一次初始检查
        self._check_all_wan_status()
        
        while not self._stop_event.is_set():
            try:
                # 等待下一个检查周期
                if self._stop_event.wait(self.monitor_interval):
                    break
                
                # 检查所有WAN状态
                self._check_all_wan_status()
                
            except Exception as e:
                if not self.silent:
                    logging.error(f"WAN监控过程中出错: {str(e)}")
                
                # 短暂暂停后继续
                if self._stop_event.wait(5):
                    break
    
    def _check_all_wan_status(self):
        """
        检查所有WAN接口状态
        """
        if not self.enabled:
            return
        
        with self.lock:
            for i in list(self.wan_status.keys()):
                self._check_wan_status(i)
    
    def _check_wan_status(self, wan_idx: int):
        """
        检查指定WAN接口状态
        
        Args:
            wan_idx: WAN接口索引
        """
        if wan_idx not in self.wan_status:
            return
        
        if wan_idx >= len(self.interfaces) or not self.interfaces[wan_idx].get('enabled', True):
            # 接口不可用或已禁用
            self.wan_status[wan_idx]['status'] = 'disabled'
            self.wan_status[wan_idx]['last_check'] = time.time()
            return
        
        # 获取接口信息
        interface = self.interfaces[wan_idx]
        wan_ip = interface.get('ip')
        
        if not wan_ip:
            # 没有配置IP
            self.wan_status[wan_idx]['status'] = 'invalid'
            self.wan_status[wan_idx]['last_check'] = time.time()
            return
        
        # 记录检查次数
        self.wan_status[wan_idx]['check_count'] += 1
        self.wan_status[wan_idx]['last_check'] = time.time()
        
        # 测试接口连通性
        status, latency = self._test_wan_connection(wan_idx, wan_ip)
        
        # 更新状态
        prev_status = self.wan_status[wan_idx].get('status')
        self.wan_status[wan_idx]['status'] = status
        self.wan_status[wan_idx]['latency'] = latency
        
        if status == 'up':
            self.wan_status[wan_idx]['success_count'] += 1
            if prev_status != 'up':
                self.wan_status[wan_idx]['up_since'] = time.time()
                if not self.silent:
                    logging.info(f"WAN {wan_idx} ({interface.get('name', f'wan{wan_idx}')}) 连接恢复，延迟: {latency:.2f}ms")
        else:
            self.wan_status[wan_idx]['fail_count'] += 1
            if prev_status == 'up':
                self.wan_status[wan_idx]['down_since'] = time.time()
                if not self.silent:
                    logging.warning(f"WAN {wan_idx} ({interface.get('name', f'wan{wan_idx}')}) 连接中断")
    
    def _test_wan_connection(self, wan_idx: int, wan_ip: str) -> Tuple[str, float]:
        """
        测试WAN接口连通性
        
        Args:
            wan_idx: WAN接口索引
            wan_ip: WAN接口IP
            
        Returns:
            Tuple[str, float]: (状态, 延迟(ms))
        """
        best_latency = float('inf')
        success = False
        
        # 测试多个目标，取最佳结果
        for target in self.test_targets:
            try:
                # 使用ping测试连通性
                latency = self._ping_with_source(target, wan_ip)
                if latency > 0:
                    success = True
                    best_latency = min(best_latency, latency)
                    
                # 如果已经成功，不需要测试更多目标
                if success and not self.silent:
                    logging.debug(f"WAN {wan_idx} 可以访问 {target}，延迟: {latency:.2f}ms")
                    break
                    
            except Exception as e:
                if not self.silent:
                    logging.debug(f"WAN {wan_idx} 无法访问 {target}: {str(e)}")
                continue
        
        if success:
            return 'up', best_latency
        else:
            return 'down', -1
    
    def _ping_with_source(self, target: str, source_ip: str) -> float:
        """
        使用指定源IP执行ping
        
        Args:
            target: 目标IP或域名
            source_ip: 源IP
            
        Returns:
            float: 延迟(ms)，失败返回-1
        """
        try:
            # 根据系统类型选择合适的ping命令
            system = platform.system().lower()
            
            if system == 'windows':
                # Windows
                cmd = ["ping", "-n", "1", "-w", str(int(self.ping_timeout * 1000)), target]
            elif system == 'darwin':
                # macOS
                cmd = ["ping", "-c", "1", "-t", str(self.ping_timeout), "-S", source_ip, target]
            else:
                # Linux and others
                cmd = ["ping", "-c", "1", "-W", str(self.ping_timeout), "-I", source_ip, target]
            
            # 执行命令
            start_time = time.time()
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=self.ping_timeout + 2)
            end_time = time.time()
            
            # 检查结果
            if result.returncode == 0:
                # 成功，计算延迟
                latency = (end_time - start_time) * 1000  # 转换为毫秒
                return latency
            else:
                # 失败
                return -1
                
        except subprocess.TimeoutExpired:
            # 命令超时
            return -1
        except Exception as e:
            if not self.silent:
                logging.debug(f"Ping失败 ({source_ip} -> {target}): {str(e)}")
            return -1
    
    def get_wan_info(self, wan_idx: int) -> Optional[Dict[str, Any]]:
        """
        获取WAN接口信息
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            Dict: WAN接口信息，不存在则返回None
        """
        with self.lock:
            if not self.enabled or wan_idx not in self.wan_status:
                return None
                
            # 如果状态很旧，触发一次检查
            last_check = self.wan_status[wan_idx].get('last_check', 0)
            if time.time() - last_check > self.monitor_interval * 2:
                threading.Thread(target=self._check_wan_status, args=(wan_idx,)).start()
            
            return self.wan_status[wan_idx].copy()
    
    def get_all_wan_info(self) -> Dict[int, Dict[str, Any]]:
        """
        获取所有WAN接口信息
        
        Returns:
            Dict[int, Dict]: {wan_idx: wan_info, ...}
        """
        with self.lock:
            if not self.enabled:
                return {}
                
            # 复制状态信息避免竞态条件
            return {idx: info.copy() for idx, info in self.wan_status.items()}
    
    def get_available_wan_indices(self) -> List[int]:
        """
        获取所有可用的WAN接口索引
        
        Returns:
            List[int]: WAN接口索引列表
        """
        with self.lock:
            if not self.enabled:
                return []
                
            available = []
            for idx, info in self.wan_status.items():
                if info.get('status') == 'up':
                    available.append(idx)
            
            return available
    
    def force_check(self, wan_idx: Optional[int] = None):
        """
        强制检查WAN接口状态
        
        Args:
            wan_idx: 需要检查的接口索引，None表示检查所有接口
        """
        if not self.enabled:
            return
            
        with self.lock:
            if wan_idx is not None:
                # 检查单个接口
                if wan_idx in self.wan_status:
                    threading.Thread(target=self._check_wan_status, args=(wan_idx,)).start()
            else:
                # 检查所有接口
                threading.Thread(target=self._check_all_wan_status).start()
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """
        获取监控状态信息
        
        Returns:
            Dict: 监控状态信息
        """
        with self.lock:
            status = {
                'enabled': self.enabled,
                'monitor_interval': self.monitor_interval,
                'is_monitoring': self._monitor_thread is not None and self._monitor_thread.is_alive(),
                'wan_count': len(self.wan_status),
                'available_count': len([idx for idx, info in self.wan_status.items() if info.get('status') == 'up']),
                'test_targets': self.test_targets,
                'last_update': time.time()
            }
            
            return status
    
    def restart_monitoring(self):
        """
        重启监控
        """
        self._stop_monitoring()
        self._start_monitoring()
    
    def __del__(self):
        """
        析构函数
        """
        self._stop_monitoring()


# 创建全局WAN监控器实例
wan_monitor = WanMonitor()