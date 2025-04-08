"""
多WAN端口分配器模块,负责为不同WAN接口分配端口
"""
import threading
import random
from typing import Dict, List, Tuple, Optional, Set
from loguru import logger
from config import config_manager


class PortAllocator:
    """
    端口分配器类,为多WAN连接分配本地端口
    """
    
    def __init__(self, silent=False):
        """
        初始化端口分配器
        
        Args:
            silent: 静默模式，不输出日志
        """
        self.silent = silent
        self.wan_port_ranges = self._parse_port_ranges()
        self.allocated_ports: Dict[int, Set[int]] = {idx: set() for idx in self.wan_port_ranges.keys()}
        self.lock = threading.Lock()
        
        # 检查配置有效性
        if not self.wan_port_ranges:
            logger.warning("未找到有效的WAN端口范围配置")
        elif not self.silent:
            logger.info(f"已加载 {len(self.wan_port_ranges)} 个WAN接口的端口范围")
            for wan_idx, (start, end) in self.wan_port_ranges.items():
                logger.debug(f"WAN {wan_idx} 端口范围: {start}-{end}")
    
    def _parse_port_ranges(self) -> Dict[int, Tuple[int, int]]:
        """
        解析配置中的端口范围
        
        Returns:
            字典,键为WAN索引,值为(起始端口,结束端口)元组
        """
        port_ranges = {}
        
        # 获取WAN配置
        wan_config = config_manager.get_wan_config()
        
        # 如果有全局端口范围定义
        global_port_range = wan_config.get('port_range')
        if global_port_range and isinstance(global_port_range, list) and len(global_port_range) == 2:
            start_port, end_port = global_port_range
            # 设置默认WAN端口范围
            port_ranges[0] = (int(start_port), int(end_port))
            
        # 处理interfaces中的端口范围
        interfaces = wan_config.get('interfaces', [])
        for idx, interface in enumerate(interfaces):
            if interface.get('enabled', False):
                port_range = interface.get('port_range')
                if port_range and isinstance(port_range, list) and len(port_range) == 2:
                    start_port, end_port = port_range
                    port_ranges[idx] = (int(start_port), int(end_port))
                    
        return port_ranges
        
        return port_ranges
    
    def get_available_wan_indices(self) -> List[int]:
        """
        获取可用的WAN接口索引
        
        Returns:
            可用WAN接口索引列表
        """
        return list(self.wan_port_ranges.keys())
    
    def allocate_port(self, wan_idx: int) -> Optional[int]:
        """
        为指定WAN接口分配一个可用端口
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            分配的端口号,如果无法分配则返回None
        """
        if wan_idx not in self.wan_port_ranges:
            logger.warning(f"WAN索引 {wan_idx} 不存在")
            return None
            
        with self.lock:
            start_port, end_port = self.wan_port_ranges[wan_idx]
            allocated = self.allocated_ports[wan_idx]
            
            if not self.silent:
                logger.debug(f"WAN {wan_idx} 端口范围: {start_port}-{end_port}, 已分配端口数: {len(allocated)}")
            
            # 计算可用端口
            available_ports = set(range(start_port, end_port + 1)) - allocated
            
            if not available_ports:
                logger.warning(f"WAN {wan_idx} 没有可用端口")
                return None
                
            # 随机选择一个可用端口
            port = random.choice(list(available_ports))
            allocated.add(port)
            
            if not self.silent:
                logger.debug(f"为WAN {wan_idx} 分配端口 {port} (范围: {start_port}-{end_port})")
            return port
    
    def release_port(self, wan_idx: int, port: int) -> bool:
        """
        释放已分配的端口
        
        Args:
            wan_idx: WAN接口索引
            port: 要释放的端口号
            
        Returns:
            是否成功释放
        """
        if wan_idx not in self.allocated_ports:
            return False
            
        with self.lock:
            if port in self.allocated_ports[wan_idx]:
                self.allocated_ports[wan_idx].remove(port)
                if not self.silent:
                    logger.debug(f"已释放WAN {wan_idx} 的端口 {port}")
                return True
            return False

# 创建全局端口分配器实例
port_allocator = PortAllocator(silent=False) 