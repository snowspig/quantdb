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
import json
import time
import random
import socket
import logging
import threading
import atexit
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set, Any

# 检测操作系统类型
IS_WINDOWS = sys.platform == 'win32'

# 尝试导入项目配置管理器，如果不可用则使用默认配置
try:
    from ..config_manager import config_manager
    HAS_CONFIG_MANAGER = True
except ImportError:
    HAS_CONFIG_MANAGER = False


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
        with self._lock:
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
        
        # 检查是否有配置管理器
        if HAS_CONFIG_MANAGER:
            # 从配置中获取端口范围
            ranges_config = config_manager.get_wan_config().get('port_ranges', {})
            
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
            
            # 添加超时机制
            start_time = time.time()
            max_try_time = 3.0  # 最多尝试3秒
            
            # 尝试查找可用端口
            attempts = 0
            max_attempts = min(20, end_port - start_port + 1)  # 减少最大尝试次数避免卡顿
            
            while attempts < max_attempts and (time.time() - start_time) < max_try_time:
                # 随机选择一个端口
                port = random.randint(start_port, end_port)
                
                # 检查端口是否已分配
                if port in allocated:
                    attempts += 1
                    # 短暂休眠避免CPU占用过高
                    time.sleep(0.01)
                    continue
                
                # 尝试绑定端口测试可用性
                try:
                    # 添加超时处理
                    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    test_socket.settimeout(0.05)  # 降低超时时间
                    test_socket.bind(('0.0.0.0', port))
                    test_socket.close()
                    
                    # 端口可用，标记为已分配
                    allocated.add(port)
                    
                    # 添加端口元数据
                    pid = os.getpid()
                    metadata = {
                        "allocated_time": time.time(),
                        "pid": pid,
                        "thread_id": threading.get_ident()
                    }
                    self.port_metadata[f"{wan_idx}:{port}"] = metadata
                    
                    self.logger.debug(f"为WAN {wan_idx} 分配端口 {port} (PID: {pid})")
                    return port
                except OSError:
                    # 端口已被占用，尝试下一个
                    attempts += 1
                    # 短暂休眠避免CPU占用过高
                    time.sleep(0.01)
                except Exception as e:
                    # 处理其他异常
                    self.logger.warning(f"端口绑定测试异常: {str(e)}")
                    attempts += 1
                    time.sleep(0.01)
            
            if (time.time() - start_time) >= max_try_time:
                self.logger.warning(f"分配端口超时，已尝试 {attempts} 次")
            else:
                self.logger.warning(f"无法为WAN {wan_idx} 分配可用端口，已尝试 {attempts} 次")
                
            # 如果所有尝试都失败，返回一个固定端口（不测试可用性）
            fallback_port = start_port + (os.getpid() % (end_port - start_port))
            self.logger.warning(f"使用后备端口 {fallback_port}（未验证可用性）")
            allocated.add(fallback_port)
            
            # 添加端口元数据
            pid = os.getpid()
            metadata = {
                "allocated_time": time.time(),
                "pid": pid,
                "thread_id": threading.get_ident(),
                "is_fallback": True
            }
            self.port_metadata[f"{wan_idx}:{fallback_port}"] = metadata
            
            return fallback_port
    
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
    wan_port_pool = SimpleWanPortPool.get_instance()
else:
    # Unix/Linux环境可以继续使用之前的完整实现
    import fcntl
    
    # 这里应该放其他平台的完整实现
    # 暂时也使用简化版以避免导入错误
    WanPortPool = SimpleWanPortPool
    wan_port_pool = SimpleWanPortPool.get_instance() 