"""
WAN管理器实用工具模块
提供系统参数优化和网络连接相关功能
"""
import os
import sys
import subprocess
import socket
import ctypes
import platform
import re
import time
import threading
from typing import Dict, Optional, List, Union, Callable, Any
from loguru import logger

# 导入通用工具函数
from core.utils import (
    # 基础工具函数
    retry, timeit, ensure_dir, list_files, 
    get_cpu_count, load_json, save_json,
    
    # 网络相关函数
    is_valid_ip, is_valid_ipv4, is_valid_ipv6,
    is_local_ip, get_local_ip
)

# TCP参数优化相关常量
# Windows系统下的TCP参数设置
WIN_TCP_TIME_WAIT_DELAY = 30  # 秒，推荐值范围: 30-60
WIN_MAX_USER_PORT = 65534  # 最大用户端口号
WIN_TCP_TW_REUSE = 1  # 开启TIME_WAIT重用
WIN_TCP_TW_RECYCLE = 0  # 默认关闭TIME_WAIT快速回收，可能导致NAT环境下的连接问题

# 端口扫描与管理常量
PORT_SCAN_TIMEOUT = 0.5  # 端口扫描超时时间（秒）
PORT_RANGE_DEFAULT = (10000, 65000)  # 默认端口范围
PORT_SCAN_BATCH_SIZE = 100  # 批量扫描时的批次大小

# 连接管理常量
CONNECTION_RETRY_COUNT = 3  # 连接重试次数
CONNECTION_RETRY_DELAY = 1.0  # 连接重试延迟（秒）
CONNECTION_TIMEOUT = 10.0  # 连接超时时间（秒）

# 全局连接计数字典和锁
_connection_count = {}
_connection_lock = threading.RLock()

def is_admin() -> bool:
    """
    检查当前进程是否具有管理员权限
    
    Returns:
        bool: 是否具有管理员权限
    """
    try:
        if sys.platform == 'win32':
            return ctypes.windll.shell32.IsUserAnAdmin() != 0
        else:
            # Unix系统下检查是否为root用户
            return os.geteuid() == 0
    except Exception:
        return False

def check_win_tcp_settings() -> Dict[str, Union[int, str]]:
    """
    检查Windows系统的TCP设置
    
    Returns:
        Dict: 当前TCP设置参数
    """
    if sys.platform != 'win32':
        logger.warning("此函数仅适用于Windows系统")
        return {}
        
    results = {}
    try:
        # 检查TcpTimedWaitDelay设置
        cmd = 'reg query "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v TcpTimedWaitDelay'
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            match = re.search(r'TcpTimedWaitDelay\s+REG_DWORD\s+0x([0-9a-f]+)', stdout.decode('utf-8', errors='ignore'), re.IGNORECASE)
            if match:
                results['TcpTimedWaitDelay'] = int(match.group(1), 16)
            else:
                results['TcpTimedWaitDelay'] = "未设置(默认240秒)"
        else:
            results['TcpTimedWaitDelay'] = "未设置(默认240秒)"
        
        # 检查MaxUserPort设置
        cmd = 'reg query "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v MaxUserPort'
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            match = re.search(r'MaxUserPort\s+REG_DWORD\s+0x([0-9a-f]+)', stdout.decode('utf-8', errors='ignore'), re.IGNORECASE)
            if match:
                results['MaxUserPort'] = int(match.group(1), 16)
            else:
                results['MaxUserPort'] = "未设置(默认5000)"
        else:
            results['MaxUserPort'] = "未设置(默认5000)"
            
        # 检查TcpNumConnections设置
        cmd = 'reg query "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v TcpNumConnections'
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            match = re.search(r'TcpNumConnections\s+REG_DWORD\s+0x([0-9a-f]+)', stdout.decode('utf-8', errors='ignore'), re.IGNORECASE)
            if match:
                results['TcpNumConnections'] = int(match.group(1), 16)
            else:
                results['TcpNumConnections'] = "未设置(默认)"
        else:
            results['TcpNumConnections'] = "未设置(默认)"
            
    except Exception as e:
        logger.error(f"检查TCP设置时出错: {str(e)}")
        results['error'] = str(e)
        
    return results

def optimize_windows_tcp_settings() -> Dict[str, Union[bool, str]]:
    """
    优化Windows TCP设置，减少TIME_WAIT状态持续时间和增加可用端口
    
    Returns:
        Dict: 操作结果
    """
    if sys.platform != 'win32':
        logger.warning("此函数仅适用于Windows系统")
        return {"success": False, "reason": "非Windows系统"}
        
    # 检查管理员权限
    if not is_admin():
        return {
            "success": False, 
            "reason": "需要管理员权限才能修改TCP设置",
            "solution": "请以管理员身份运行程序或手动修改注册表设置"
        }
        
    results = {"success": True, "changes": []}
    try:
        # 设置TcpTimedWaitDelay为30秒
        cmd = f'reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v TcpTimedWaitDelay /t REG_DWORD /d {WIN_TCP_TIME_WAIT_DELAY} /f'
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            results["changes"].append(f"设置TcpTimedWaitDelay={WIN_TCP_TIME_WAIT_DELAY}秒")
        else:
            results["success"] = False
            results["changes"].append(f"设置TcpTimedWaitDelay失败: {stderr.decode('utf-8', errors='ignore')}")
        
        # 设置MaxUserPort为65534
        cmd = f'reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v MaxUserPort /t REG_DWORD /d {WIN_MAX_USER_PORT} /f'
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            results["changes"].append(f"设置MaxUserPort={WIN_MAX_USER_PORT}")
        else:
            results["success"] = False
            results["changes"].append(f"设置MaxUserPort失败: {stderr.decode('utf-8', errors='ignore')}")
            
        # 提示需要重启计算机使设置生效
        results["notice"] = "设置已应用，但需要重启计算机才能生效"
        results["current_settings"] = check_win_tcp_settings()
        
    except Exception as e:
        logger.error(f"优化TCP设置时出错: {str(e)}")
        results["success"] = False
        results["error"] = str(e)
        
    return results

def get_tcp_optimization_guide() -> str:
    """
    获取TCP优化指南
    
    Returns:
        str: 优化指南文本
    """
    if sys.platform == 'win32':
        guide = """
Windows系统TCP优化指南:

1. 减少TIME_WAIT状态持续时间 (TcpTimedWaitDelay)
   - 默认值: 240秒
   - 建议值: 30-60秒
   - 修改方法: 管理员权限运行命令提示符，执行:
     reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v TcpTimedWaitDelay /t REG_DWORD /d 30 /f

2. 增加可用端口范围 (MaxUserPort)
   - 默认值: 5000
   - 建议值: 65534
   - 修改方法: 管理员权限运行命令提示符，执行:
     reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v MaxUserPort /t REG_DWORD /d 65534 /f

3. 应用设置:
   - 所有设置修改后需重启计算机生效

当前系统设置:
"""
        # 添加当前系统设置
        settings = check_win_tcp_settings()
        for key, value in settings.items():
            guide += f"- {key}: {value}\n"
            
        return guide
    else:
        return "当前系统不是Windows，此优化指南不适用。"

def get_system_info() -> Dict[str, str]:
    """
    获取系统信息
    
    Returns:
        Dict: 系统信息
    """
    info = {
        "os": platform.system(),
        "os_version": platform.version(),
        "os_release": platform.release(),
        "architecture": platform.architecture()[0],
        "processor": platform.processor(),
        "hostname": socket.gethostname(),
        "python_version": platform.python_version(),
    }
    
    if sys.platform == 'win32':
        info["tcp_settings"] = check_win_tcp_settings()
        
    return info

# 连接管理相关函数
def track_connection(host: str, port: int) -> None:
    """
    跟踪一个新的连接
    
    Args:
        host: 主机地址
        port: 端口号
    """
    key = f"{host}:{port}"
    with _connection_lock:
        if key in _connection_count:
            _connection_count[key] += 1
        else:
            _connection_count[key] = 1
            
def release_connection(host: str, port: int) -> None:
    """
    释放一个连接
    
    Args:
        host: 主机地址
        port: 端口号
    """
    key = f"{host}:{port}"
    with _connection_lock:
        if key in _connection_count and _connection_count[key] > 0:
            _connection_count[key] -= 1
            
def get_connection_count(host: Optional[str] = None, port: Optional[int] = None) -> Union[int, Dict[str, int]]:
    """
    获取连接计数
    
    Args:
        host: 可选，指定主机地址
        port: 可选，指定端口号
        
    Returns:
        int或Dict: 连接计数或全部连接计数字典
    """
    with _connection_lock:
        if host is not None and port is not None:
            key = f"{host}:{port}"
            return _connection_count.get(key, 0)
        return dict(_connection_count)
        
def connection_manager(func: Callable) -> Callable:
    """
    连接管理装饰器，自动跟踪和释放连接
    
    Args:
        func: 要装饰的函数，第一个参数为host，第二个参数为port
        
    Returns:
        装饰后的函数
    """
    @retry(max_attempts=CONNECTION_RETRY_COUNT, delay=CONNECTION_RETRY_DELAY)
    def wrapper(*args, **kwargs):
        host = args[0] if len(args) > 0 else kwargs.get('host')
        port = args[1] if len(args) > 1 else kwargs.get('port')
        
        if host and port:
            track_connection(host, port)
            try:
                return func(*args, **kwargs)
            finally:
                release_connection(host, port)
        else:
            return func(*args, **kwargs)
            
    return wrapper

# 端口扫描相关函数
def scan_port_range(host: str, start_port: int, end_port: int, timeout: float = PORT_SCAN_TIMEOUT) -> List[int]:
    """
    扫描主机的端口范围，返回开放的端口列表
    
    Args:
        host: 主机地址
        start_port: 起始端口
        end_port: 结束端口
        timeout: 超时时间（秒）
        
    Returns:
        List[int]: 开放的端口列表
    """
    if not is_valid_ip(host):
        logger.error(f"无效的IP地址: {host}")
        return []
        
    open_ports = []
    
    # 分批扫描以提高效率
    for batch_start in range(start_port, end_port + 1, PORT_SCAN_BATCH_SIZE):
        batch_end = min(batch_end + PORT_SCAN_BATCH_SIZE - 1, end_port)
        threads = []
        results = [False] * PORT_SCAN_BATCH_SIZE
        
        for i, port in enumerate(range(batch_start, batch_end + 1)):
            thread = threading.Thread(
                target=lambda idx, p: results.__setitem__(idx, not check_port_availability(host, p, timeout)),
                args=(i, port)
            )
            threads.append(thread)
            thread.start()
            
        for thread in threads:
            thread.join()
            
        for i, is_open in enumerate(results):
            if is_open and i + batch_start <= end_port:
                open_ports.append(i + batch_start)
                
    return open_ports

def check_port_availability(host: str, port: int, timeout: float = PORT_SCAN_TIMEOUT) -> bool:
    """
    检查指定主机的端口是否可用（未被占用）
    
    Args:
        host: 主机地址
        port: 端口号
        timeout: 超时时间（秒）
        
    Returns:
        bool: 端口是否可用
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            result = s.connect_ex((host, port))
            return result != 0  # 0表示连接成功，端口已被占用
    except Exception as e:
        logger.error(f"检查端口可用性出错: {host}:{port}, 错误: {str(e)}")
        return False

def get_available_port(start_port: int = PORT_RANGE_DEFAULT[0], 
                       end_port: int = PORT_RANGE_DEFAULT[1]) -> int:
    """
    获取本地可用端口
    
    Args:
        start_port: 起始端口
        end_port: 结束端口
        
    Returns:
        int: 可用端口，如果没有找到则返回-1
    """
    for port in range(start_port, end_port + 1):
        if check_port_availability("127.0.0.1", port):
            return port
    return -1

# 网络连接测试函数
def test_connection(host: str, port: int, 
                    timeout: float = CONNECTION_TIMEOUT,
                    retry_count: int = CONNECTION_RETRY_COUNT) -> Dict[str, Any]:
    """
    测试到指定主机和端口的连接
    
    Args:
        host: 主机地址
        port: 端口号
        timeout: 超时时间（秒）
        retry_count: 重试次数
        
    Returns:
        Dict: 连接测试结果
    """
    if not is_valid_ip(host):
        return {
            "success": False,
            "error": f"无效的IP地址: {host}",
            "time_ms": 0
        }
        
    result = {
        "success": False,
        "host": host,
        "port": port,
        "time_ms": 0,
        "attempts": 0
    }
    
    for attempt in range(1, retry_count + 1):
        result["attempts"] = attempt
        
        try:
            start_time = time.time()
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                s.connect((host, port))
                end_time = time.time()
                result["time_ms"] = round((end_time - start_time) * 1000, 2)
                result["success"] = True
                break
        except socket.timeout:
            result["error"] = "连接超时"
        except ConnectionRefusedError:
            result["error"] = "连接被拒绝"
        except Exception as e:
            result["error"] = f"连接错误: {str(e)}"
            
        if attempt < retry_count:
            time.sleep(CONNECTION_RETRY_DELAY)
            
    return result

# 导出的函数列表，用于帮助自动完成和文档生成
__all__ = [
    # 导入的通用函数
    'retry', 'timeit', 'ensure_dir', 'list_files',
    'get_cpu_count', 'load_json', 'save_json',
    
    # 导入的网络函数
    'is_valid_ip', 'is_valid_ipv4', 'is_valid_ipv6',
    'is_local_ip', 'get_local_ip',
    
    # WAN管理器特有的函数
    'is_admin', 'check_win_tcp_settings', 'optimize_windows_tcp_settings',
    'get_tcp_optimization_guide', 'get_system_info',
    'check_port_availability', 'get_available_port',
    
    # 连接管理函数
    'track_connection', 'release_connection', 'get_connection_count',
    'connection_manager', 'test_connection',
    
    # 端口扫描函数
    'scan_port_range',
    
    # 常量
    'WIN_TCP_TIME_WAIT_DELAY', 'WIN_MAX_USER_PORT',
    'WIN_TCP_TW_REUSE', 'WIN_TCP_TW_RECYCLE',
    'PORT_SCAN_TIMEOUT', 'PORT_RANGE_DEFAULT', 'PORT_SCAN_BATCH_SIZE',
    'CONNECTION_RETRY_COUNT', 'CONNECTION_RETRY_DELAY', 'CONNECTION_TIMEOUT'
] 