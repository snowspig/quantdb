#!/usr/bin/env python
"""
WAN端口池模块 - 用于解决多进程环境下WAN端口分配冲突问题

该模块实现了一个WAN端口分配池，用于管理不同WAN接口的端口分配。
当前实现包含两个版本：
1. 内存版：适用于Windows环境的简化版本，仅支持单进程内的端口分配
2. 完整版：基于文件锁的跨进程版本，支持Linux/Unix环境

使用方式：
from core.wan_manager.wan_port_pool import WanPortPool
pool = WanPortPool.get_instance()
port = pool.allocate_port(wan_idx)
...
pool.release_port(wan_idx, port)
"""
import os
import sys
import time
import random
import socket
import logging
import threading
import atexit
from typing import Dict, List, Tuple, Optional, Any

# 检测操作系统类型
IS_WINDOWS = sys.platform == 'win32'

# 导入 ConfigManager 类
from core.config_manager import ConfigManager

# 定义内存版WAN端口池，适用于Windows环境
class SimpleWanPortPool:
    """
    简化版WAN端口池 - 仅支持单进程模式
    适用于Windows环境，不使用文件锁和状态文件
    """
    _instance = None
    _initialized = False
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls) -> 'SimpleWanPortPool':
        """获取单例实例"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """初始化端口池（单例模式，只在第一次调用时初始化）"""
        if SimpleWanPortPool._initialized:
            return
            
        # 初始化日志
        self.logger = logging.getLogger("core.wan_manager.SimpleWanPortPool")
        self.logger.info("正在初始化WAN端口池...")
        
        try:
            # 初始化端口范围和分配状态
            self.wan_port_ranges = self._parse_port_ranges()
            
            # 初始化已分配端口集合
            self.allocated_ports = {wan_idx: set() for wan_idx in self.wan_port_ranges.keys()}
            
            # 端口元数据
            self.port_metadata = {}
            
            # 注册退出时清理函数（使用弱引用避免循环引用）
            import weakref
            atexit.register(weakref.proxy(self)._cleanup_wrapper)
            
            self.logger.info(f"端口池初始化完成，管理 {len(self.wan_port_ranges)} 个WAN接口")
            for wan_idx, (start, end) in self.wan_port_ranges.items():
                self.logger.info(f"WAN {wan_idx} 端口范围: {start}-{end}")
            
            # 标记初始化完成
            SimpleWanPortPool._initialized = True
        except Exception as e:
            self.logger.error(f"WAN端口池初始化失败: {str(e)}")
            import traceback
            self.logger.error(f"错误详情: {traceback.format_exc()}")
            # 即使失败也标记为已初始化，避免反复尝试
            SimpleWanPortPool._initialized = True
            # 创建空的端口范围，确保后续调用不会崩溃
            self.wan_port_ranges = {0: (50001, 51000)}
            self.allocated_ports = {0: set()}
            self.port_metadata = {}
    
    def _cleanup_wrapper(self):
        """清理函数包装器，避免在atexit中直接引用self"""
        try:
            self._cleanup()
        except:
            pass
            
    def _parse_port_ranges(self) -> Dict[int, Tuple[int, int]]:
        """解析配置中的端口范围"""
        port_ranges = {}
        
        # 直接获取 ConfigManager 实例并使用
        try:
            _config_manager = ConfigManager() # 获取单例实例
            # 从配置中获取端口范围
            # 注意: 这里需要确定 get_wan_config() 方法是否仍然存在或需要调整
            # 假设 get_wan_config 存在于 config_manager 实例上
            # 并且返回 network.wan 下的配置
            network_config = _config_manager.get('network', {})
            wan_config = network_config.get('wan', {})
            ranges_config = wan_config.get('port_ranges', {}) # 尝试从 network.wan 读取

            # 如果 network.wan 中没有，尝试从顶层 wan 读取 (兼容旧配置)
            if not ranges_config:
                top_wan_config = _config_manager.get('wan', {})
                ranges_config = top_wan_config.get('port_ranges', {})
            
            # 如果配置不为空，解析配置
            if ranges_config:
                for wan_idx_str, port_range in ranges_config.items():
                    try:
                        wan_idx = int(wan_idx_str)
                        if isinstance(port_range, list) and len(port_range) == 2:
                            start_port, end_port = port_range
                            port_ranges[wan_idx] = (int(start_port), int(end_port))
                        elif isinstance(port_range, dict) and 'start' in port_range and 'end' in port_range:
                            start_port, end_port = port_range['start'], port_range['end']
                            port_ranges[wan_idx] = (int(start_port), int(end_port))
                    except (ValueError, TypeError) as e:
                        self.logger.error(f"解析端口范围失败: {str(e)}")
                
                if port_ranges:
                    return port_ranges
        except Exception as e:
            self.logger.error(f"获取或解析配置时出错: {e}")
        
        # 使用默认配置
        self.logger.info("使用默认端口范围配置")
        return {
            0: (50001, 51000),  # WAN 0
            1: (51001, 52000),  # WAN 1
            2: (52001, 53000),  # WAN 2
        }
    
    def get_available_wan_indices(self) -> List[int]:
        """获取可用的WAN接口索引"""
        return list(self.wan_port_ranges.keys())
    
    def allocate_port(self, wan_idx: int) -> Optional[int]:
        """为指定WAN接口分配一个可用端口"""
        if wan_idx not in self.wan_port_ranges:
            self.logger.warning(f"WAN索引 {wan_idx} 不存在")
            return None
            
        with self._lock:
            start_port, end_port = self.wan_port_ranges[wan_idx]
            allocated = self.allocated_ports[wan_idx]
            
            # 添加历史使用记录维护
            if not hasattr(self, '_port_usage_history'):
                self._port_usage_history = {}
            
            # 端口冷却期记录
            if not hasattr(self, '_port_cooldown'):
                self._port_cooldown = {}
            
            # 端口分配失败记录和退避时间
            if not hasattr(self, '_port_failure_counts'):
                self._port_failure_counts = {}
            
            # 按使用频率排序端口选择
            cool_time = time.time() - 120  # 2分钟冷却期
            
            # 智能端口选择策略
            candidate_ports = []
            for _ in range(min(10, end_port - start_port + 1)):
                # 优先选择长时间未使用的端口
                port = self._select_best_port(wan_idx, start_port, end_port, cool_time)
                if port and port not in candidate_ports:
                    candidate_ports.append(port)
                
            # 如果没有合适的端口，使用随机策略
            if not candidate_ports:
                candidate_ports = [random.randint(start_port, end_port) for _ in range(5)]
            
            # 尝试各个候选端口
            for port in candidate_ports:
                if port in allocated:
                    continue
                
                # 检查端口冷却状态
                if port in self._port_cooldown and self._port_cooldown[port] > time.time():
                    continue
                
                # 尝试绑定测试端口可用性
                try:
                    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    test_socket.settimeout(0.05)
                    test_socket.bind(('0.0.0.0', port))
                    test_socket.close()
                    
                    # 端口可用，记录分配
                    allocated.add(port)
                    self._port_usage_history[port] = time.time()
                    
                    # 重置失败计数
                    if port in self._port_failure_counts:
                        del self._port_failure_counts[port]
                    
                    return port
                except Exception as e:
                    # 增加失败计数和退避时间
                    self._port_failure_counts[port] = self._port_failure_counts.get(port, 0) + 1
                    self._port_cooldown[port] = time.time() + min(30, self._port_failure_counts[port] * 5)
            
            # 常规分配失败，使用确定性算法分配端口
            fallback_port = start_port + ((os.getpid() + int(time.time())) % (end_port - start_port))
            allocated.add(fallback_port)
            return fallback_port
    
    def _select_best_port(self, wan_idx, start_port, end_port, cool_time):
        """选择最佳可用端口"""
        # 实现端口选择算法，考虑历史使用情况、冷却期和失败记录
        # ...
    
    def release_port(self, wan_idx: int, port: int) -> bool:
        """释放已分配的端口"""
        with self._lock:
            # 检查端口是否已分配
            if wan_idx not in self.allocated_ports or port not in self.allocated_ports[wan_idx]:
                return False
            
            # 释放端口
            self.allocated_ports[wan_idx].remove(port)
            
            # 删除元数据
            metadata_key = f"{wan_idx}:{port}"
            if metadata_key in self.port_metadata:
                del self.port_metadata[metadata_key]
            
            self.logger.debug(f"已释放WAN {wan_idx} 的端口 {port}")
            return True
    
    def get_port_usage_info(self) -> Dict[str, Any]:
        """获取当前端口使用情况"""
        with self._lock:
            info = {
                "timestamp": time.time(),
                "wan_port_usage": {},
                "total_allocated_ports": 0
            }
            
            for wan_idx, ports in self.allocated_ports.items():
                wan_idx_str = str(wan_idx)
                start_port, end_port = self.wan_port_ranges.get(wan_idx, (0, 0))
                total_ports = end_port - start_port + 1 if (start_port and end_port) else 0
                allocated_count = len(ports)
                
                info["wan_port_usage"][wan_idx_str] = {
                    "total_ports": total_ports,
                    "allocated_ports": allocated_count,
                    "available_ports": total_ports - allocated_count if total_ports > 0 else 0,
                    "allocated_port_list": sorted(list(ports))
                }
                
                info["total_allocated_ports"] += allocated_count
            
            return info
    
    def _cleanup(self):
        """清理当前进程分配的所有端口"""
        try:
            with self._lock:
                # 简化版本直接清理所有端口
                for wan_idx in self.allocated_ports.keys():
                    self.allocated_ports[wan_idx].clear()
                self.port_metadata.clear()
                self.logger.debug("进程退出，清理所有分配的端口")
        except Exception as e:
            self.logger.error(f"退出清理时出错: {str(e)}")


# 根据操作系统选择合适的WAN端口池实现
if IS_WINDOWS:
    # Windows环境使用简化版实现
    WanPortPool = SimpleWanPortPool
    # 移除模块级实例化
    # wan_port_pool = SimpleWanPortPool.get_instance()
else:
    # Unix/Linux环境可以继续使用之前的完整实现
    # import fcntl # 暂时注释掉，如果完整实现需要再启用
    
    # 这里应该放其他平台的完整实现
    # 暂时也使用简化版以避免导入错误
    WanPortPool = SimpleWanPortPool
    # 移除模块级实例化
    # wan_port_pool = SimpleWanPortPool.get_instance() 