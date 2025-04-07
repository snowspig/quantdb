"""
WAN端口分配器模块，负责管理多WAN环境下的端口分配
"""
import socket
import threading
import random
from typing import Dict, List, Tuple, Optional, Set
import time
from loguru import logger
import os
import json

# 从配置管理器导入
from config import config_manager


class WanPortAllocator:
    """
    WAN端口分配器类，管理多WAN环境下的端口分配，支持并行数据获取
    """
    
    def __init__(self, config_path=None, silent=False):
        """
        初始化WAN端口分配器
        
        Args:
            config_path: 配置文件路径，不提供则使用默认配置
            silent: 是否静默模式（不输出日志）
        """
        self.config_path = config_path
        self.silent = silent
        self.lock = threading.Lock()
        
        # WAN接口相关配置
        self.wan_enabled = config_manager.is_wan_enabled()
        self.wan_interfaces = {}  # {wan_idx: {'ip': '192.168.1.x', 'status': 'up'}}
        self.wan_port_ranges = {}  # {wan_idx: (start_port, end_port)}
        self.allocated_ports = {}  # {wan_idx: {port1, port2, ...}}
        self.wan_sockets = {}  # {(wan_idx, port): socket}
        
        # 加载WAN配置
        self._load_wan_config()
        
        if not self.silent:
            if self.wan_enabled:
                logger.info(f"WAN端口分配器初始化完成，已加载 {len(self.wan_interfaces)} 个接口")
            else:
                logger.info("多WAN功能未启用")
    
    def _load_wan_config(self):
        """
        加载WAN配置信息
        """
        if not self.wan_enabled:
            if not self.silent:
                logger.debug("多WAN功能未启用，跳过配置加载")
            return
        
        # 从配置中加载WAN接口信息
        wan_config = config_manager.get_wan_config() or {}
        wan_interfaces = wan_config.get("interfaces", {})
        port_ranges = wan_config.get("port_ranges", {})
        
        # 检查接口状态文件
        status_file = wan_config.get("status_file", "wan_ip_checks.json")
        if not os.path.isabs(status_file):
            status_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), status_file)
        
        interfaces = {}
        # 加载接口状态
        try:
            if os.path.exists(status_file):
                with open(status_file, 'r') as f:
                    status_data = json.load(f)
                    for wan_idx, status_info in status_data.items():
                        wan_idx = int(wan_idx)
                        status = status_info.get("status", "down")
                        ip = status_info.get("ip")
                        if ip and status.lower() == "up":
                            interfaces[wan_idx] = {"ip": ip, "status": "up"}
                            if not self.silent:
                                logger.debug(f"已加载WAN接口 {wan_idx}，IP: {ip}")
        except Exception as e:
            if not self.silent:
                logger.error(f"加载WAN状态文件失败: {e}")
        
        # 加载端口范围
        port_ranges_dict = {}
        for wan_idx, port_range in port_ranges.items():
            try:
                wan_idx = int(wan_idx)
                if isinstance(port_range, list) and len(port_range) == 2:
                    start_port, end_port = port_range
                    port_ranges_dict[wan_idx] = (int(start_port), int(end_port))
            except (ValueError, TypeError) as e:
                if not self.silent:
                    logger.error(f"解析端口范围失败: {str(e)}")
        
        # 初始化已分配端口集合
        allocated_ports = {idx: set() for idx in port_ranges_dict.keys()}
        
        # 设置实例变量
        self.wan_interfaces = interfaces
        self.wan_port_ranges = port_ranges_dict
        self.allocated_ports = allocated_ports
        
        if not self.silent:
            logger.debug(f"已加载 {len(interfaces)} 个可用WAN接口")
            logger.debug(f"已配置 {len(port_ranges_dict)} 个端口范围")
    
    def get_available_wan_indices(self) -> List[int]:
        """
        获取可用的WAN接口索引
        
        Returns:
            可用WAN接口索引列表
        """
        if not self.wan_enabled:
            return []
        
        with self.lock:
            # 返回状态为up的接口索引
            return [idx for idx, info in self.wan_interfaces.items() 
                   if info.get("status") == "up" and idx in self.wan_port_ranges]
    
    def allocate_port(self, wan_idx: int) -> Tuple[int, int]:
        """
        为指定WAN接口分配一个可用端口
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            Tuple[int, int]: (wan_idx, 分配的端口号)，如果无法分配则返回(None, None)
        """
        if not self.wan_enabled or wan_idx not in self.wan_interfaces or wan_idx not in self.wan_port_ranges:
            if not self.silent:
                logger.warning(f"WAN索引 {wan_idx} 不可用或不存在")
            return None, None
        
        with self.lock:
            # 检查接口状态
            if self.wan_interfaces[wan_idx].get("status") != "up":
                if not self.silent:
                    logger.warning(f"WAN接口 {wan_idx} 状态不是'up'")
                return None, None
            
            # 获取端口范围
            start_port, end_port = self.wan_port_ranges[wan_idx]
            allocated = self.allocated_ports.get(wan_idx, set())
            
            if not self.silent:
                logger.debug(f"WAN {wan_idx} 端口范围: {start_port}-{end_port}, 已分配端口数: {len(allocated)}")
            
            # 计算可用端口
            available_ports = set(range(start_port, end_port + 1)) - allocated
            
            if not available_ports:
                if not self.silent:
                    logger.warning(f"WAN {wan_idx} 没有可用端口")
                return None, None
            
            # 随机选择一个可用端口
            port = random.choice(list(available_ports))
            allocated.add(port)
            self.allocated_ports[wan_idx] = allocated
            
            if not self.silent:
                logger.debug(f"为WAN {wan_idx} 分配端口 {port}")
            
            # 尝试创建并绑定套接字
            try:
                # 获取接口IP
                if_ip = self.wan_interfaces[wan_idx].get("ip")
                if not if_ip:
                    if not self.silent:
                        logger.warning(f"WAN接口 {wan_idx} 无有效IP地址")
                    return None, None
                
                # 创建套接字并绑定
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((if_ip, port))
                
                # 存储套接字引用
                self.wan_sockets[(wan_idx, port)] = sock
                
                if not self.silent:
                    logger.debug(f"成功为WAN {wan_idx} (IP: {if_ip}) 绑定端口 {port}")
                
                return wan_idx, port
            
            except Exception as e:
                # 绑定失败，释放端口
                self.allocated_ports[wan_idx].remove(port)
                if not self.silent:
                    logger.error(f"为WAN {wan_idx} 绑定端口 {port} 失败: {str(e)}")
                return None, None
    
    def release_port(self, wan_idx: int, port: int) -> bool:
        """
        释放已分配的端口
        
        Args:
            wan_idx: WAN接口索引
            port: 要释放的端口号
            
        Returns:
            是否成功释放
        """
        if not self.wan_enabled or wan_idx not in self.allocated_ports:
            return False
        
        with self.lock:
            # 检查端口是否已分配
            if port in self.allocated_ports[wan_idx]:
                # 关闭并释放套接字
                sock_key = (wan_idx, port)
                if sock_key in self.wan_sockets:
                    try:
                        self.wan_sockets[sock_key].close()
                        del self.wan_sockets[sock_key]
                    except Exception as e:
                        if not self.silent:
                            logger.warning(f"关闭WAN {wan_idx} 端口 {port} 的套接字时出错: {str(e)}")
                
                # 从已分配集合中移除
                self.allocated_ports[wan_idx].remove(port)
                
                if not self.silent:
                    logger.debug(f"已释放WAN {wan_idx} 的端口 {port}")
                return True
            return False
    
    def get_socket(self, wan_idx: int, port: int) -> Optional[socket.socket]:
        """
        获取指定WAN接口和端口的套接字
        
        Args:
            wan_idx: WAN接口索引
            port: 端口号
            
        Returns:
            对应的套接字对象，如果不存在则返回None
        """
        return self.wan_sockets.get((wan_idx, port))
    
    def cleanup_ports(self) -> None:
        """
        清理所有已分配的端口
        """
        with self.lock:
            # 关闭所有套接字
            for (wan_idx, port), sock in list(self.wan_sockets.items()):
                try:
                    sock.close()
                except Exception as e:
                    if not self.silent:
                        logger.warning(f"关闭WAN {wan_idx} 端口 {port} 的套接字时出错: {str(e)}")
            
            # 清空集合
            self.wan_sockets.clear()
            for wan_idx in list(self.allocated_ports.keys()):
                self.allocated_ports[wan_idx].clear()
            
            if not self.silent:
                logger.debug("已清理所有已分配的端口")
    
    def __del__(self):
        """
        析构函数，确保释放所有资源
        """
        self.cleanup_ports()


# 创建全局WAN端口分配器实例
wan_port_allocator = WanPortAllocator()