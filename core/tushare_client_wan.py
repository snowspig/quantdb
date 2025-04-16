#!/usr/bin/env python
"""
多WAN环境下的Tushare客户端
支持多WAN口绑定请求的Tushare API客户端

该客户端允许通过不同的WAN接口发送请求，提高请求成功率和并发能力
"""

import time
import json
import pandas as pd
import requests
from typing import Dict, List, Optional, Tuple
from loguru import logger
import socket
import random
import threading
import subprocess
import platform
import re

# 导入WAN管理组件 - 改为导入 getter 函数
# from .wan_manager import wan_port_pool
from .wan_manager import get_wan_port_pool # 导入 getter

# 添加端口检查锁，避免多线程同时检查同一端口
_port_check_lock = threading.Lock()
_checked_ports = set()

# 添加端口互斥锁字典，为每个使用中的端口创建一个锁
_port_locks = {}
_port_locks_lock = threading.RLock()

# 添加端口冷却期记录
_port_cooldown = {}
_port_cooldown_lock = threading.Lock()
_PORT_COOLDOWN_TIME = 60  # 增加到60秒，减少端口复用频率

# 添加端口使用计数器，跟踪端口使用情况
_port_usage_count = {}
_port_usage_lock = threading.Lock()

# 获取端口锁函数
def get_port_lock(port: int) -> threading.RLock:
    """获取指定端口的互斥锁"""
    with _port_locks_lock:
        if port not in _port_locks:
            _port_locks[port] = threading.RLock()
        return _port_locks[port]

# 增加端口使用计数
def increment_port_usage(port: int) -> int:
    """增加端口使用计数并返回当前计数"""
    with _port_usage_lock:
        if port not in _port_usage_count:
            _port_usage_count[port] = 0
        _port_usage_count[port] += 1
        return _port_usage_count[port]

# 减少端口使用计数
def decrement_port_usage(port: int) -> int:
    """减少端口使用计数并返回当前计数"""
    with _port_usage_lock:
        if port in _port_usage_count:
            _port_usage_count[port] = max(0, _port_usage_count[port] - 1)
            return _port_usage_count[port]
        return 0

# 端口检查函数
def is_port_available(port: int) -> bool:
    """检查端口是否可用"""
    # 只检查端口是否在冷却期，不使用计数器预检查
    with _port_cooldown_lock:
        if port in _port_cooldown and time.time() - _port_cooldown[port] < _PORT_COOLDOWN_TIME:
            return False
    
    # 直接尝试绑定，通过实际结果判断可用性
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(0.5)
        sock.bind(('0.0.0.0', port))
        sock.close()
        return True
    except OSError:
        return False

# 清理端口检查记录
def clear_port_check(port: int) -> None:
    """从已检查端口集合中清除端口记录"""
    global _checked_ports
    with _port_check_lock:
        if port in _checked_ports:
            _checked_ports.remove(port)
    
    # 将端口添加到冷却期记录
    with _port_cooldown_lock:
        _port_cooldown[port] = time.time()
        logger.debug(f"端口 {port} 已添加到冷却期")

# 清理过期的冷却期记录
def clean_cooldown_records():
    """清理已过冷却期的端口记录"""
    with _port_cooldown_lock:
        current_time = time.time()
        expired_ports = [
            port for port, cooldown_time in _port_cooldown.items() 
            if current_time - cooldown_time >= _PORT_COOLDOWN_TIME
        ]
        for port in expired_ports:
            del _port_cooldown[port]
        if expired_ports:
            logger.debug(f"已清理 {len(expired_ports)} 个过期冷却端口记录")

# 检查Windows系统的TIME_WAIT设置
def check_time_wait_settings():
    """检查系统TIME_WAIT设置，仅在Windows系统下有效"""
    if platform.system() != "Windows":
        logger.debug("非Windows系统，跳过TIME_WAIT设置检查")
        return
    
    try:
        # 获取当前TIME_WAIT延迟设置
        cmd = "netsh int tcp show global"
        result = subprocess.check_output(cmd, shell=True, text=True)
        
        # 解析结果
        time_wait_match = re.search(r"TCP [Tt]imed [Ww]ait [Dd]elay[^\d]*(\d+)", result)
        if time_wait_match:
            delay = int(time_wait_match.group(1))
            logger.info(f"当前系统TIME_WAIT延迟设置为：{delay}秒")
            
            if delay > 30:
                logger.warning(
                    f"TIME_WAIT延迟设置({delay}秒)过长，可能导致端口资源耗尽。"
                    f"建议使用管理员权限运行命令：netsh int tcp set global timed_wait_delay=30"
                )
        else:
            logger.debug("无法解析TIME_WAIT延迟设置")
    
    except Exception as e:
        logger.error(f"检查TIME_WAIT设置时出错: {e}")

# 导入WAN绑定适配器
class SourceAddressAdapter(requests.adapters.HTTPAdapter):
    """HTTP适配器，支持设置源地址"""
    
    def __init__(self, source_address, **kwargs):
        self.source_address = source_address
        super(SourceAddressAdapter, self).__init__(**kwargs)
        
    def init_poolmanager(self, *args, **kwargs):
        kwargs['source_address'] = self.source_address
        super(SourceAddressAdapter, self).init_poolmanager(*args, **kwargs)

# 异常类定义
class RequestFailedException(Exception):
    """请求失败异常"""
    pass

class WanIndexNotFoundException(Exception):
    """WAN索引不存在异常"""
    pass


class TushareClientWAN:
    """
    多WAN环境下的Tushare客户端
    
    通过指定不同的WAN接口与服务器通信，提高请求成功率
    支持参数化请求和结果处理，自动重试和错误处理
    """
    
    def __init__(self, token: str, api_url: str = "http://116.128.206.39:7172", wan_idx: Optional[int] = None, timeout: int = 60):
        """
        初始化WAN绑定的Tushare客户端
        
        Args:
            token: Tushare API令牌
            api_url: API URL地址，默认为湘财Tushare API地址
            wan_idx: WAN接口索引,不指定则随机选择
            timeout: 请求超时时间(秒)
        """
        self.token = token
        self.url = api_url
        self.timeout = timeout
        self.port = None
        self.wan_idx = wan_idx # 初始化时可以设置默认 wan_idx
        
        # 获取可用WAN索引 - 改为调用 getter
        _wan_port_pool = get_wan_port_pool()
        if _wan_port_pool:
            self.available_wan_indices = _wan_port_pool.get_available_wan_indices()
        else:
            logger.error("无法获取 WAN 端口池实例！")
            self.available_wan_indices = []
        
        # 如果未指定WAN索引,随机选择一个
        if wan_idx is None:
            if not self.available_wan_indices:
                raise WanIndexNotFoundException("无可用WAN接口索引")
            self.wan_idx = random.choice(self.available_wan_indices)
        elif wan_idx not in self.available_wan_indices:
            raise WanIndexNotFoundException(f"WAN索引 {wan_idx} 不存在")
            
        self.logger = logger.bind(context="TushareClientWAN")
        # 不再在初始化时固定日志的 wan_idx，让请求方法自己记录
        # self.logger.info(f"初始化为WAN索引 {self.wan_idx}") 
        
        # HTTP会话相关
        self.headers = {
            "Content-Type": "application/json",
        }
        self.proxies = None
        self.local_addr = None
        
        # 每个WAN实例可能会被多个线程共享，需要线程安全
        self._port_lock = threading.RLock()
        
        # 验证token
        mask_token = token[:4] + '*' * (len(token) - 8) + token[-4:] if len(token) > 8 else '***'
        logger.debug(f"TushareClientWAN初始化: {mask_token} (长度: {len(token)}), API URL: {self.url}")
        
        # 在初始化时检查TIME_WAIT设置
        check_time_wait_settings()
    
    def set_local_address(self, host: str, port: int):
        """设置本地地址绑定"""
        if port <= 0:
            logger.warning(f"无效的端口号 {port}，不进行本地地址绑定")
            self.local_addr = None
            return
        
        # 先检查端口是否可用
        if not is_port_available(port):
            logger.warning(f"端口 {port} 可能已被占用，不进行绑定")
            self.local_addr = None
            return

        self.local_addr = (host, port)
        logger.debug(f"已设置本地地址绑定: {host}:{port}")
        
        # 在set_local_address成功时增加计数
        if self.local_addr == (host, port):
            increment_port_usage(port)
    
    def reset_local_address(self):
        """重置本地地址绑定"""
        # 如果有本地地址，尝试清理端口检查记录
        if self.local_addr and isinstance(self.local_addr, tuple) and len(self.local_addr) == 2:
            _, port = self.local_addr
            clear_port_check(port)
            
        self.local_addr = None
        logger.debug("已重置本地地址绑定")
        
        # 在reset_local_address时减少计数
        if self.local_addr and isinstance(self.local_addr, tuple):
            _, port = self.local_addr
            decrement_port_usage(port)
    
    def set_timeout(self, timeout: int):
        """设置请求超时"""
        self.timeout = timeout
        logger.debug(f"已设置请求超时: {timeout}秒")
        
    def __del__(self):
        """析构函数,释放分配的端口"""
        self._release_port(self.wan_idx, self.port)
    
    def _ensure_port(self, wan_idx: int) -> int:
        """
        确保为指定的 WAN 索引分配端口
        
        Args:
            wan_idx: 要分配端口的 WAN 索引
        
        Returns:
            分配的端口号
        """
        with self._port_lock: # 锁住实例变量 port 的修改
            if self.port is None:
                _wan_port_pool = get_wan_port_pool()
                if not _wan_port_pool:
                    raise RequestFailedException("无法获取 WAN 端口池实例")
                # 使用传入的 wan_idx 分配端口
                self.port = _wan_port_pool.allocate_port(wan_idx)
                self.logger.debug(f"为WAN {wan_idx} 分配端口 {self.port}")
                if self.port is None:
                    raise RequestFailedException(f"无法为WAN {wan_idx} 分配端口")
            # 注意：这里返回的是实例当前的 port，调用者需要确保这是为其 wan_idx 分配的
            # 或者考虑让这个方法不修改 self.port，仅分配并返回
            return self.port
    
    def _release_port(self, wan_idx: int, port_to_release: int):
        """
        释放指定的端口 (如果它属于指定的 WAN)
        不再依赖 self.port
        
        Args:
            wan_idx: 端口所属的 WAN 索引
            port_to_release: 要释放的端口号
        """
        with self._port_lock: # 仍然需要锁来安全地修改 self.port（如果当前端口被释放）
            if port_to_release is not None:
                _wan_port_pool = get_wan_port_pool()
                if _wan_port_pool:
                    released = _wan_port_pool.release_port(wan_idx, port_to_release)
                    if released:
                        self.logger.debug(f"释放WAN {wan_idx} 的端口 {port_to_release}")
                        # 如果释放的是当前实例持有的端口，则重置 self.port
                        if self.port == port_to_release:
                            self.port = None
                    else:
                         self.logger.warning(f"端口池未能释放 WAN {wan_idx} 的端口 {port_to_release}")
                else:
                     self.logger.warning("无法获取 WAN 端口池实例来释放端口")
            # 如果 port_to_release 是 None，则不做任何事
            # 如果 self.port 是要释放的端口，重置它
            if self.port == port_to_release:
                self.port = None
    
    def _recreate_port(self, wan_idx: int) -> int:
        """为指定 WAN 重新创建端口"""
        with self._port_lock:
            # 先释放当前实例可能持有的旧端口（无论属于哪个WAN）
            current_instance_port = self.port 
            if current_instance_port is not None: 
                 self._release_port(self.wan_idx, current_instance_port) # 使用实例的 wan_idx 释放
            # 然后为指定的 wan_idx 分配新端口
            return self._ensure_port(wan_idx)
    
    def get_data(self, api_name: str, params: dict = None, fields: list = None, 
                retry_count: int = 3, wan_idx: Optional[int] = None) -> pd.DataFrame: # 新增 wan_idx 参数
        """增强版数据获取方法，使用指定的 wan_idx"""
        
        # 确定要使用的 wan_idx
        target_wan_idx = wan_idx if wan_idx is not None else self.wan_idx
        if target_wan_idx is None or target_wan_idx not in self.available_wan_indices:
            if not self.available_wan_indices:
                 logger.error(f"请求 {api_name}: 无可用 WAN 接口，无法继续。")
                 return None
            target_wan_idx = random.choice(self.available_wan_indices)
            logger.warning(f"请求 {api_name}: 未指定有效 WAN 索引，随机选择 {target_wan_idx}")
            
        request_id = f"req_{target_wan_idx}_{time.time():.2f}_{random.randint(100,999)}"
        temp_local_addr = None
        port_lock = None
        has_port_lock = False  # 新增锁状态标志
        binding_success = False
        binding_attempts = 0
        max_binding_attempts = 10  # 增加到10次尝试
        bound_port_for_release = None
        acquired_lock_for_release = None
        
        # 该WAN口已尝试过的端口集合
        tried_ports = set()
        
        try:
            # 创建请求数据 - 与原始TushareClient请求格式保持一致
            req_params = {
                "api_name": api_name,
                "token": self.token,
                "params": params or {},
                "fields": fields or ""
            }
            
            logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 请求URL: {self.url}, API: {api_name}")
            
            # 使用requests发送请求
            start_time = time.time()
            
            # 创建Session，添加重试机制
            s = requests.Session()
            retry = requests.adapters.Retry(
                total=2,  # 总共重试2次
                backoff_factor=0.3,  # 重试间隔增长因子
                status_forcelist=[500, 502, 503, 504]
            )
            adapter = requests.adapters.HTTPAdapter(max_retries=retry)
            s.mount('http://', adapter)
            s.mount('https://', adapter)
            
            # --- 端口分配和绑定重试逻辑 (使用 target_wan_idx) ---
            # 获取WAN口端口池实例并获取端口范围
            _wan_port_pool = get_wan_port_pool()
            if not _wan_port_pool:
                logger.error(f"[{request_id}] (WAN {target_wan_idx}) 无法获取端口池实例")
                # 尝试使用默认模式进行请求
                binding_success = False
            else:
                # 获取该WAN口的端口范围
                port_range = _wan_port_pool.get_port_range(target_wan_idx) if hasattr(_wan_port_pool, 'get_port_range') else None
                if port_range and len(port_range) == 2:
                    start_port, end_port = port_range
                    logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 端口范围: {start_port}-{end_port}")
                else:
                    # 如果无法获取端口范围，使用默认范围
                    if target_wan_idx == 0:
                        start_port, end_port = 50001, 51000
                    elif target_wan_idx == 1:
                        start_port, end_port = 51001, 52000
                    elif target_wan_idx == 2:
                        start_port, end_port = 52001, 53000
                    else:
                        # 其他WAN索引，使用默认范围
                        start_port, end_port = 50001 + (target_wan_idx * 1000), 51000 + (target_wan_idx * 1000)
                    logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 使用默认端口范围: {start_port}-{end_port}")
                
                while binding_attempts < max_binding_attempts and not binding_success:
                    binding_attempts += 1
                    port_lock = None
                    has_port_lock = False  # 重置锁状态
                    current_port = None # 重置当前尝试的端口
                    logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 第 {binding_attempts}/{max_binding_attempts} 次尝试端口分配与绑定...")
                    
                    # 1. 尝试获取一个端口
                    try:
                        if _wan_port_pool:
                            # 从WAN端口池获取端口
                            current_port = _wan_port_pool.allocate_port(target_wan_idx)
                        
                        # 如果通过端口池无法获取端口（或者获取的端口已经尝试过），尝试直接分配
                        if not current_port or current_port in tried_ports:
                            # 如果该WAN口的所有端口都尝试过了，重置已尝试端口集合
                            if len(tried_ports) >= (end_port - start_port) * 0.9:  # 如果尝试了90%的端口，重置
                                logger.warning(f"[{request_id}] (WAN {target_wan_idx}) 已尝试大多数可用端口，重置端口尝试状态")
                                tried_ports.clear()
                            
                            # 随机选择一个未尝试过的端口
                            available_ports = set(range(start_port, end_port + 1)) - tried_ports
                            if available_ports:
                                current_port = random.choice(list(available_ports))
                                logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 直接分配随机端口: {current_port}")
                            else:
                                logger.warning(f"[{request_id}] (WAN {target_wan_idx}) 无法找到可用端口，跳过当前尝试")
                                time.sleep(0.2)  # 短暂等待
                                continue
                        
                        # 记录已尝试的端口
                        tried_ports.add(current_port)
                        logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: 获取到端口 {current_port}")
                    except Exception as alloc_err:
                        logger.error(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: 分配端口时出错: {alloc_err}")
                        time.sleep(0.2)  # 短暂等待后继续
                        continue

                    # 2. 检查端口是否可用
                    if not is_port_available(current_port):
                        logger.warning(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: 端口 {current_port} 不可用 (占用/冷却)，立即尝试下一个端口")
                        self._release_port(target_wan_idx, current_port) # 释放这个特定端口
                        current_port = None
                        # 不休眠，立即尝试下一个端口
                        continue

                    # 3. 尝试获取端口锁
                    port_lock = get_port_lock(current_port)
                    try:
                        has_port_lock = port_lock.acquire(timeout=0.5)  # 缩短超时时间
                        if not has_port_lock:
                            logger.warning(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: 无法获取端口 {current_port} 的锁，立即尝试下一个端口")
                            self._release_port(target_wan_idx, current_port) # 释放这个特定端口
                            current_port = None
                            port_lock = None
                            # 不休眠，立即尝试下一个端口
                            continue
                        
                        # 4. 尝试设置绑定地址
                        try:
                            increment_port_usage(current_port)
                            logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: 已获取锁，尝试绑定端口 {current_port}")
                            # 改回使用0.0.0.0绑定
                            self.set_local_address('0.0.0.0', current_port) # set_local_address 影响的是实例状态
                            
                            # 5. 验证绑定是否成功
                            if self.local_addr == ('0.0.0.0', current_port):
                                binding_success = True
                                bound_port_for_release = current_port
                                acquired_lock_for_release = port_lock
                                logger.info(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: 成功绑定到端口 {current_port}")
                                break
                            else:
                                # 如果 set_local_address 没有成功更新 self.local_addr
                                logger.warning(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: set_local_address 未成功设置端口 {current_port}，立即尝试下一个端口")
                                decrement_port_usage(current_port)
                                port_lock.release()
                                has_port_lock = False
                                port_lock = None
                                self._release_port(target_wan_idx, current_port) # 释放这个特定端口
                                current_port = None
                                # 不休眠，立即尝试下一个端口
                                continue
                        except Exception as bind_err:
                            # 绑定过程中出错
                            logger.error(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: 绑定端口 {current_port} 时出错: {bind_err}，立即尝试下一个端口")
                            decrement_port_usage(current_port)
                            port_lock.release()
                            has_port_lock = False
                            port_lock = None
                            self._release_port(target_wan_idx, current_port) # 释放这个特定端口
                            current_port = None
                            # 不休眠，立即尝试下一个端口
                            continue
                    except Exception as lock_err:
                        # 获取锁过程中出错
                        logger.error(f"[{request_id}] (WAN {target_wan_idx}) 尝试 {binding_attempts}: 获取端口锁时出错: {lock_err}，立即尝试下一个端口")
                        self._release_port(target_wan_idx, current_port) # 释放这个特定端口
                        current_port = None
                        port_lock = None
                        # 不休眠，立即尝试下一个端口
                        continue

            # --- 结束端口分配和绑定重试逻辑 ---
                            
            # 尝试不同的请求策略
            response = None
            error_msg = None
            
            # 1. 如果绑定成功，尝试绑定模式
            if binding_success and self.local_addr:
                try:
                    host, current_port = self.local_addr
                    source_adapter = SourceAddressAdapter((host, current_port))
                    s.mount('http://', source_adapter)
                    s.mount('https://', source_adapter)
                    logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 使用本地地址绑定: {host}:{current_port}")
                    
                    response = s.post(
                        self.url,
                        json=req_params,
                        headers=self.headers,
                        timeout=self.timeout,
                        proxies=self.proxies
                    )
                except OSError as e:
                    if "Address already in use" in str(e):
                        clear_port_check(current_port)
                        error_msg = f"端口绑定冲突: {str(e)}"
                        logger.error(f"[{request_id}] {error_msg}")
                        binding_success = False
                    else:
                        error_msg = f"本地地址绑定请求失败: {str(e)}"
                        logger.error(f"[{request_id}] {error_msg}")
                        binding_success = False
                except Exception as e:
                    error_msg = f"请求异常: {str(e)}"
                    logger.error(f"[{request_id}] {error_msg}")
                    binding_success = False
            
            # 2. 如果绑定不成功或绑定模式失败，尝试普通模式
            if not binding_success:
                # 确保之前的端口锁已释放（如果获取过）
                if has_port_lock and port_lock is not None:
                    try:
                        port_lock.release()
                        has_port_lock = False
                    except Exception as release_err:
                        logger.warning(f"[{request_id}] 释放端口锁失败: {release_err}")
                    port_lock = None
                # 确保 local_addr 已重置
                self.reset_local_address()
                
                try:
                    # 创建新的会话，不使用绑定
                    ns = requests.Session()
                    ns.mount('http://', adapter)
                    ns.mount('https://', adapter)
                    
                    logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 尝试使用默认网络连接 (绑定失败或未尝试)")
                    
                    # 等待短暂时间再尝试，避免与其他请求冲突
                    # 使用随机退避算法增加错开时间
                    backoff_time = 0.1 + random.random() * 0.2  # 减少退避时间
                    time.sleep(backoff_time)
                    
                    response = ns.post(
                        self.url,
                        json=req_params,
                        headers=self.headers,
                        timeout=self.timeout,
                        proxies=self.proxies
                    )
                except Exception as e:
                    error_msg = f"默认连接模式请求失败: {str(e)}"
                    logger.error(f"[{request_id}] {error_msg}")
                    # 如果到这里还是失败，接下来就无法继续了，抛出异常
                    raise
            
            elapsed = time.time() - start_time
            logger.debug(f"[{request_id}] (WAN {target_wan_idx}) API请求耗时: {elapsed:.2f}s")
            
            # 检查响应状态
            if response.status_code != 200:
                logger.error(f"[{request_id}] API请求错误: {response.status_code} - {response.text}")
                return None
                
            # 解析响应
            result = response.json()
            if result.get('code') != 0:
                logger.error(f"[{request_id}] API返回错误: {result.get('code')} - {result.get('msg')}")
                return None
                
            # 转换为DataFrame
            data = result.get('data')
            if not data or not data.get('items'):
                logger.debug(f"[{request_id}] API返回空数据")
                return pd.DataFrame()
                
            items = data.get('items')
            columns = data.get('fields')
            
            # 创建DataFrame
            df = pd.DataFrame(items, columns=columns)
            logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 成功获取数据，行数: {len(df)}")
            return df
            
        except Exception as e:
            logger.error(f"[{request_id}] 获取API数据失败: {str(e)}")
            import traceback
            logger.debug(f"[{request_id}] 详细错误信息: {traceback.format_exc()}")
            return None
        finally:
            # 释放成功绑定的端口和锁 (使用 target_wan_idx)
            if bound_port_for_release:
                try:
                    count = decrement_port_usage(bound_port_for_release)
                    logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 请求结束，释放端口 {bound_port_for_release}，使用计数: {count}")
                    self._release_port(target_wan_idx, bound_port_for_release) # 使用正确的wan_idx释放
                    with _port_cooldown_lock:
                        _port_cooldown[bound_port_for_release] = time.time()
                except Exception as port_err:
                    logger.warning(f"[{request_id}] 释放端口失败: {port_err}")
                
            if acquired_lock_for_release:
                try:
                    # 确保锁确实被获取了并且仍然持有
                    if hasattr(acquired_lock_for_release, '_is_owned') and acquired_lock_for_release._is_owned():
                        acquired_lock_for_release.release()
                        logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 请求结束，释放端口锁 for {bound_port_for_release}")
                    else:
                        logger.debug(f"[{request_id}] (WAN {target_wan_idx}) 端口锁未被获取或已释放，跳过释放操作")
                except Exception as lock_err:
                    logger.warning(f"[{request_id}] 释放端口锁失败: {lock_err}")
            
            # 重置实例的绑定状态
            self.reset_local_address()
            
            # 清理冷却记录
            if random.random() < 0.1:
                clean_cooldown_records()
    
    def _make_request(self, api_name: str, params: dict, fields: List[str] = None, 
                     retry_count: int = 3, retry_interval: float = 1.0) -> dict:
        """
        发送请求到Tushare API
        
        Args:
            api_name: API名称
            params: 参数字典
            fields: 请求的字段列表
            retry_count: 重试次数
            retry_interval: 重试间隔(秒)
            
        Returns:
            API响应
        """
        # 构建请求体
        req_params = {
            'api_name': api_name,
            'token': self.token,
            'params': params
        }
        if fields:
            req_params['fields'] = fields
            
        # 确保端口已分配
        port = self._ensure_port(self.wan_idx)
        
        # 在请求过程中获取并持有端口的全局互斥锁
        port_lock = get_port_lock(port)
        has_port_lock = False # 新增锁状态标志
        
        attempt = 0
        last_error = None
        
        try:
            has_port_lock = port_lock.acquire()
            if not has_port_lock:
                raise RequestFailedException("无法获取端口锁")
            
            while attempt < retry_count:
                attempt += 1
                try:
                    session = requests.Session()
                    session.trust_env = False # 不使用系统代理
                    
                    # 设置本地端口
                    source = ('0.0.0.0', port)
                    
                    # 发起请求
                    start_time = time.time()
                    resp = session.post(
                        self.url,
                        json=req_params,
                        timeout=self.timeout
                    )
                    duration = time.time() - start_time
                    
                    # 检查HTTP状态码
                    if resp.status_code != 200:
                        error_msg = f"HTTP错误: {resp.status_code}, 响应: {resp.text[:100]}"
                        self.logger.warning(f"请求失败 (尝试 {attempt}/{retry_count}): {error_msg}")
                        last_error = RequestFailedException(error_msg)
                        time.sleep(retry_interval)
                        continue
                        
                    # 解析响应
                    try:
                        result = resp.json()
                    except Exception as e:
                        error_msg = f"解析响应JSON失败: {str(e)}, 响应内容: {resp.text[:100]}"
                        self.logger.warning(f"请求失败 (尝试 {attempt}/{retry_count}): {error_msg}")
                        last_error = RequestFailedException(error_msg)
                        time.sleep(retry_interval)
                        continue
                        
                    # 检查API返回的错误码
                    if result.get('code') != 0:
                        error_msg = f"API错误 {result.get('code')}: {result.get('msg', '未知错误')}"
                        self.logger.warning(f"请求失败 (尝试 {attempt}/{retry_count}): {error_msg}")
                        last_error = RequestFailedException(error_msg)
                        
                        # 如果是令牌错误,不重试
                        if result.get('code') == 10001:
                            break
                            
                        # !! Force port recreate on API error !!
                        if attempt < retry_count: 
                            # 先释放锁，然后重建端口，最后重新获取锁
                            port_lock.release()
                            has_port_lock = False
                            self._recreate_port(self.wan_idx)
                            port = self.port # 获取新的端口
                            port_lock = get_port_lock(port)
                            has_port_lock = port_lock.acquire()
                            if not has_port_lock:
                                raise RequestFailedException("无法获取重建后端口的锁")
                        time.sleep(retry_interval)
                        continue
                        
                    # 请求成功
                    self.logger.debug(f"请求成功 (用时: {duration:.2f}秒, WAN: {self.wan_idx}, 端口: {port})")
                    return result
                    
                except requests.exceptions.Timeout:
                    error_msg = f"请求超时 (超时设置: {self.timeout}秒)"
                    self.logger.warning(f"请求失败 (尝试 {attempt}/{retry_count}): {error_msg}")
                    last_error = RequestFailedException(error_msg)
                    
                    # 超时后重新分配端口
                    if attempt < retry_count:
                        # 先释放锁，然后重建端口，最后重新获取锁
                        port_lock.release()
                        has_port_lock = False
                        self._recreate_port(self.wan_idx)
                        port = self.port # 获取新的端口
                        port_lock = get_port_lock(port)
                        has_port_lock = port_lock.acquire()
                        if not has_port_lock:
                            raise RequestFailedException("无法获取重建后端口的锁")
                        time.sleep(retry_interval)
                    continue # Continue the loop after recreating port
                        
                except requests.exceptions.ConnectionError as e:
                    error_msg = f"连接错误: {str(e)}"
                    self.logger.warning(f"请求失败 (尝试 {attempt}/{retry_count}): {error_msg}")
                    last_error = RequestFailedException(error_msg)
                    
                    # 连接错误后重新分配端口
                    if attempt < retry_count:
                        # 先释放锁，然后重建端口，最后重新获取锁
                        port_lock.release()
                        has_port_lock = False
                        self._recreate_port(self.wan_idx)
                        port = self.port # 获取新的端口
                        port_lock = get_port_lock(port)
                        has_port_lock = port_lock.acquire()
                        if not has_port_lock:
                            raise RequestFailedException("无法获取重建后端口的锁")
                        time.sleep(retry_interval)
                    continue # Continue the loop after recreating port
                        
                except Exception as e:
                    error_msg = f"请求异常: {str(e)}"
                    self.logger.warning(f"请求失败 (尝试 {attempt}/{retry_count}): {error_msg}")
                    last_error = RequestFailedException(error_msg)
                    # !! Force port recreate on general exception !!
                    if attempt < retry_count:
                        # 先释放锁，然后重建端口，最后重新获取锁
                        port_lock.release()
                        has_port_lock = False
                        self._recreate_port(self.wan_idx)
                        port = self.port # 获取新的端口
                        port_lock = get_port_lock(port)
                        has_port_lock = port_lock.acquire()
                        if not has_port_lock:
                            raise RequestFailedException("无法获取重建后端口的锁")
                    time.sleep(retry_interval)
                    continue # Continue the loop after recreating port
        finally:
            # 确保锁被释放
            if has_port_lock:
                try:
                    port_lock.release()
                except Exception as e:
                    self.logger.warning(f"释放端口锁失败: {e}")
        
        # 若尝试多次后仍然失败,抛出最后一次错误
        if last_error:
            self.logger.error(f"请求最终失败 (尝试 {retry_count} 次后): {str(last_error)}")
            raise last_error
            
        raise RequestFailedException("未知错误")
    
    def query(self, api_name: str, params: dict = None, fields: List[str] = None, 
             retry_count: int = 3) -> Dict:
        """
        查询Tushare API
        
        Args:
            api_name: API名称
            params: 参数字典,默认为空字典
            fields: 请求的字段列表
            retry_count: 重试次数
            
        Returns:
            API响应字典
        """
        if params is None:
            params = {}
            
        return self._make_request(api_name, params, fields, retry_count)
        
    def query_data(self, api_name: str, params: dict = None, fields: List[str] = None, 
                  retry_count: int = 3) -> Tuple[List[List], List[str]]:
        """
        查询数据并以列表形式返回
        
        Args:
            api_name: API名称
            params: 参数字典,默认为空字典
            fields: 请求的字段列表
            retry_count: 重试次数
            
        Returns:
            Tuple[List[List], List[str]]: (数据列表,字段列表)
        """
        if params is None:
            params = {}
            
        result = self._make_request(api_name, params, fields, retry_count)
        
        # 检查返回的数据结构
        if 'data' not in result:
            self.logger.warning(f"API未返回数据字段: {json.dumps(result)[:100]}")
            return [], result.get('fields', [])
        
        return result.get('data', []), result.get('fields', []) 