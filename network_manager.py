#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
网络管理器模块，集成WAN多网络管理、监控和负载均衡功能
参考wan_test_client.py中的测试方法实现
"""
import os
import sys
import time
import json
import socket
import requests
import logging
import urllib.parse
import threading
from typing import Dict, List, Any, Optional, Tuple

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入项目模块
from config import config_manager
from wan_manager.port_allocator import PortAllocator
from wan_manager.load_balancer import load_balancer
from wan_manager.wan_monitor import wan_monitor

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("network_manager")

# 测试服务器配置 - 使用指定的测试URL
SERVER_URL = "http://106.14.185.239:29990/test"

class NetworkManager:
    """网络管理器，提供多WAN网络请求和管理功能"""
    
    _instance = None
    _lock = threading.RLock()
    
    def __new__(cls, *args, **kwargs):
        """单例模式"""
        with cls._lock:
            if not cls._instance:
                cls._instance = super(NetworkManager, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance
    
    def __init__(self, config_path=None, silent=False):
        """初始化网络管理器"""
        # 单例初始化保护
        with self._lock:
            if self._initialized:
                return
            self._initialized = True
        
        self.config_path = config_path
        self.silent = silent
        
        # 加载配置
        self._load_config()
        
        # 创建端口分配器
        self.port_allocator = PortAllocator()
        
        # 初始化会话池
        self.sessions = {}
        
        # 打印WAN信息
        if not silent and self.wan_enabled:
            logger.info(f"NetworkManager: 多WAN模式已启用, {len(self.port_allocator.wan_port_ranges)}个WAN接口")
            for wan_idx, port_range in self.port_allocator.wan_port_ranges.items():
                start_port, end_port = port_range
                logger.info(f"NetworkManager: WAN {wan_idx} 端口范围 {start_port}-{end_port}")
        
        # 初始化监控
        if self.wan_enabled:
            wan_monitor.start_monitoring()
        
    def _load_config(self):
        """加载配置"""
        # 基础网络配置
        self.network_config = config_manager.get('network', {})
        self.timeout = self.network_config.get('timeout', 60)
        self.retry_count = self.network_config.get('retry_count', 3)
        
        # 代理配置
        self.proxy_config = self.network_config.get('proxy', {})
        self.proxy_enabled = self.proxy_config.get('enabled', False)
        
        # WAN配置
        self.wan_enabled = config_manager.is_wan_enabled()
        if not self.wan_enabled and not self.silent:
            logger.warning("多WAN功能未启用，将使用默认网络配置")
    
    def get_available_wans(self) -> List[int]:
        """获取可用的WAN接口列表"""
        if not self.wan_enabled:
            return []
        
        return self.port_allocator.get_available_wan_indices()
    
    def bind_socket_to_wan(self, sock: socket.socket, wan_idx: int) -> bool:
        """
        将套接字绑定到指定的WAN接口
        
        Args:
            sock: 套接字对象
            wan_idx: WAN接口索引
        
        Returns:
            bool: 是否绑定成功
        """
        if not self.wan_enabled:
            return False
        
        try:
            port = self.port_allocator.allocate_port(wan_idx)
            if port:
                logger.debug(f"为WAN {wan_idx} 分配到端口 {port}，开始绑定套接字")
                sock.bind(('0.0.0.0', port))
                logger.debug(f"套接字成功绑定到WAN {wan_idx}, 端口 {port}")
                return True
            else:
                logger.warning(f"无法为WAN {wan_idx} 分配端口")
                return False
        except Exception as e:
            logger.error(f"绑定套接字到WAN {wan_idx} 失败: {e}")
            return False
    
    def request_via_wan(self, wan_idx: int, url: str = None, method: str = 'GET',
                      headers: dict = None, params: dict = None, data: Any = None) -> dict:
        """
        通过指定WAN接口发送HTTP请求
        使用低级套接字实现，确保正确绑定到指定WAN接口
        
        Args:
            wan_idx: WAN接口索引
            url: 请求URL，默认使用SERVER_URL
            method: HTTP方法
            headers: 请求头
            params: URL参数
            data: 请求数据
        
        Returns:
            dict: 请求结果
        """
        if not self.wan_enabled:
            return {'success': False, 'error': '多WAN功能未启用'}
        
        # 使用默认URL
        if url is None:
            url = SERVER_URL
        
        logger.info(f"通过WAN {wan_idx} 发送{method}请求到 {url}")
        
        try:
            # 从URL解析主机和端口
            parsed_url = urllib.parse.urlparse(url)
            host = parsed_url.hostname
            port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 80)
            
            # 分配端口
            local_port = self.port_allocator.allocate_port(wan_idx)
            if not local_port:
                raise Exception(f"无法为WAN {wan_idx} 分配端口")
            
            # 创建套接字
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # 绑定到指定端口
                sock.bind(('0.0.0.0', local_port))
                logger.debug(f"套接字成功绑定到WAN {wan_idx}, 端口 {local_port}")
                
                # 设置超时
                sock.settimeout(self.timeout)
                
                # 连接到服务器
                start_time = time.time()
                sock.connect((host, port))
                
                # 构建URL路径和查询参数
                path = parsed_url.path or '/'
                if not path.startswith('/'):
                    path = '/' + path
                
                # 合并查询参数
                query_params = {}
                if parsed_url.query:
                    for param in parsed_url.query.split('&'):
                        if '=' in param:
                            key, value = param.split('=', 1)
                            query_params[key] = value
                
                if params:
                    query_params.update(params)
                
                # 添加默认测试参数
                query_params['wan_idx'] = str(wan_idx)
                query_params['client_time'] = str(time.time())
                
                # 构建查询字符串
                query_string = ''
                if query_params:
                    query_string = '?' + '&'.join(f"{k}={v}" for k, v in query_params.items())
                
                # 构建HTTP请求头
                request_lines = [f"{method} {path}{query_string} HTTP/1.1"]
                request_lines.append(f"Host: {host}:{port}")
                request_lines.append("Connection: close")
                
                # 添加自定义请求头
                if headers:
                    for header, value in headers.items():
                        request_lines.append(f"{header}: {value}")
                
                # 添加内容长度和内容类型（如果有数据）
                body = ""
                if data:
                    if isinstance(data, dict):
                        import json
                        body = json.dumps(data)
                        request_lines.append("Content-Type: application/json")
                    elif isinstance(data, str):
                        body = data
                    else:
                        body = str(data)
                    
                    request_lines.append(f"Content-Length: {len(body)}")
                
                # 完成请求头
                request_lines.append("")
                
                # 如果有数据，添加请求体
                if body:
                    request_lines.append(body)
                
                # 构建完整请求
                request = "\r\n".join(request_lines)
                
                # 发送请求
                sock.sendall(request.encode())
                
                # 接收响应
                response = b""
                while True:
                    try:
                        data = sock.recv(4096)
                        if not data:
                            break
                        response += data
                    except socket.timeout:
                        break
                
                # 计算时间和解析响应
                elapsed = time.time() - start_time
                response_str = response.decode('utf-8', errors='ignore')
                
                # 解析HTTP响应
                status_code = 0
                headers = {}
                body = ""
                
                # 分离响应头和响应体
                if "\r\n\r\n" in response_str:
                    header_part, body = response_str.split("\r\n\r\n", 1)
                    header_lines = header_part.split("\r\n")
                    
                    # 解析状态行
                    if header_lines and " " in header_lines[0]:
                        status_parts = header_lines[0].split(" ", 2)
                        if len(status_parts) >= 2:
                            try:
                                status_code = int(status_parts[1])
                            except ValueError:
                                pass
                    
                    # 解析响应头
                    for line in header_lines[1:]:
                        if ": " in line:
                            name, value = line.split(": ", 1)
                            headers[name.lower()] = value
                
                # 解析JSON响应（如果是JSON）
                json_data = None
                try:
                    # 尝试直接解析响应体
                    json_data = json.loads(body)
                except json.JSONDecodeError:
                    # 尝试查找JSON部分
                    json_start = body.find('{')
                    if json_start != -1:
                        try:
                            json_data = json.loads(body[json_start:])
                        except json.JSONDecodeError:
                            pass
                
                # 构建结果
                if json_data:
                    # JSON响应成功
                    logger.info(f"WAN {wan_idx} 请求成功, 状态码: {status_code}, 服务器IP: {json_data.get('your_ip')}")
                    return {
                        "wan_idx": wan_idx,
                        "success": True,
                        "status_code": status_code,
                        "elapsed": elapsed,
                        "headers": headers,
                        "server_response": json_data,
                        "ip_address": json_data.get("your_ip"),
                        "timestamp": time.time()
                    }
                elif status_code >= 200 and status_code < 300:
                    # 非JSON成功响应
                    logger.info(f"WAN {wan_idx} 请求成功, 状态码: {status_code}")
                    return {
                        "wan_idx": wan_idx,
                        "success": True,
                        "status_code": status_code,
                        "elapsed": elapsed,
                        "headers": headers,
                        "body": body,
                        "timestamp": time.time()
                    }
                else:
                    # 请求失败
                    logger.warning(f"WAN {wan_idx} 请求失败, 状态码: {status_code}")
                    return {
                        "wan_idx": wan_idx,
                        "success": False,
                        "status_code": status_code,
                        "elapsed": elapsed,
                        "headers": headers,
                        "body": body,
                        "error": f"HTTP错误: {status_code}",
                        "timestamp": time.time()
                    }
            
            finally:
                # 关闭套接字，释放端口
                sock.close()
                self.port_allocator.release_port(local_port)
        
        except Exception as e:
            logger.error(f"WAN {wan_idx} 请求异常: {str(e)}")
            return {
                "wan_idx": wan_idx,
                "success": False,
                "error": str(e),
                "timestamp": time.time()
            }
    
    def test_all_wans(self) -> Dict[str, Any]:
        """测试所有可用的WAN接口"""
        if not self.wan_enabled:
            return {"enabled": False, "message": "多WAN功能未启用"}
        
        logger.info("开始测试所有WAN接口...")
        
        # 获取可用的WAN接口
        available_wans = self.get_available_wans()
        if not available_wans:
            logger.warning("没有可用的WAN接口！")
            return {"enabled": True, "active": False, "message": "没有可用的WAN接口"}
        
        # 测试结果
        results = []
        unique_ips = set()
        
        # 逐个测试
        for wan_idx in available_wans:
            result = self.request_via_wan(wan_idx)
            results.append(result)
            time.sleep(1)  # 防止请求太快
            
            # 记录唯一IP
            if result.get("success") and result.get("ip_address"):
                unique_ips.add(result.get("ip_address"))
        
        # 分析结果
        success_count = sum(1 for r in results if r.get("success", False))
        
        logger.info(f"WAN测试完成: {success_count}/{len(results)} 成功")
        logger.info(f"服务器看到的不同IP数量: {len(unique_ips)}")
        if unique_ips:
            logger.info(f"具体IP: {', '.join(unique_ips)}")
        
        # 判断多WAN是否激活
        wan_status = {
            "enabled": True,
            "active": len(unique_ips) > 1,
            "available_wans": len(available_wans),
            "success_count": success_count,
            "unique_ip_count": len(unique_ips),
            "unique_ips": list(unique_ips),
            "results": results
        }
        
        # 加入状态信息
        if len(unique_ips) > 1 and success_count > 1:
            wan_status["message"] = "多WAN功能正常激活 - 检测到不同的出口IP"
            logger.info("✅ 多WAN功能正常激活 - 检测到不同的出口IP")
        elif success_count > 1:
            wan_status["message"] = "多WAN配置可能未正确激活 - 所有请求使用相同的IP"
            logger.warning("⚠️ 多WAN配置可能未正确激活 - 所有请求使用相同的IP")
        else:
            wan_status["message"] = "无法确定多WAN状态 - 成功请求数量不足"
            logger.error("❌ 无法确定多WAN状态 - 成功请求数量不足")
        
        # 尝试获取请求历史
        try:
            history_url = SERVER_URL.replace('/test', '/history')
            history_response = requests.get(history_url)
            if history_response.status_code == 200:
                history_data = history_response.json()
                logger.info(f"服务器记录的总请求数: {history_data.get('count', 0)}")
                wan_status["server_history"] = history_data
        except Exception as e:
            logger.error(f"获取服务器历史记录失败: {str(e)}")
        
        return wan_status
    
    def make_request(self, url: str, method: str = 'GET', wan_idx: Optional[int] = None,
                   headers: Dict = None, params: Dict = None, data: Any = None,
                   json_data: Dict = None, timeout: int = None) -> Dict[str, Any]:
        """
        发送HTTP请求，支持多WAN
        
        Args:
            url: 请求URL
            method: HTTP方法
            wan_idx: WAN接口索引，不指定则自动选择
            headers: 请求头
            params: URL参数
            data: 请求数据
            json_data: JSON请求数据
            timeout: 超时时间（秒）
        
        Returns:
            Dict: 请求结果
        """
        # 对于多WAN模式
        if self.wan_enabled:
            # 如果没有指定WAN接口，选择一个
            if wan_idx is None:
                available_wans = self.get_available_wans()
                if available_wans:
                    wan_idx = load_balancer.select_wan()
            
            # 如果有有效的WAN接口，使用低级套接字实现
            if wan_idx is not None:
                return self.request_via_wan(
                    wan_idx=wan_idx,
                    url=url,
                    method=method,
                    headers=headers,
                    params=params,
                    data=json_data if json_data else data
                )
        
        # 对于非多WAN模式，或者多WAN模式但无法分配WAN接口的情况，使用requests
        logger.debug(f"使用标准网络发送{method}请求到 {url}")
        try:
            # 设置超时
            if timeout is None:
                timeout = self.timeout
            
            # 发送请求
            start_time = time.time()
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                data=data,
                json=json_data,
                timeout=timeout
            )
            elapsed = time.time() - start_time
            
            # 尝试解析JSON
            json_data = None
            try:
                json_data = response.json()
            except:
                pass
            
            # 构建结果
            result = {
                "success": response.status_code < 400,
                "status_code": response.status_code,
                "elapsed": elapsed,
                "headers": dict(response.headers),
                "timestamp": time.time()
            }
            
            # 添加响应体
            if json_data:
                result["server_response"] = json_data
                # 如果是测试服务器的响应，添加IP
                if "your_ip" in json_data:
                    result["ip_address"] = json_data["your_ip"]
            else:
                result["body"] = response.text
            
            return result
            
        except Exception as e:
            logger.error(f"请求异常: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": time.time()
            }
    
    def get(self, url: str, **kwargs) -> Dict[str, Any]:
        """发送GET请求"""
        return self.make_request(url=url, method="GET", **kwargs)
    
    def post(self, url: str, **kwargs) -> Dict[str, Any]:
        """发送POST请求"""
        return self.make_request(url=url, method="POST", **kwargs)
    
    def save_results(self, results, filename="network_manager_results.json"):
        """保存测试结果到文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info(f"测试结果已保存到 {filename}")


# 创建单例实例
network_manager = NetworkManager()


def main():
    """主函数"""
    logger.info("NetworkManager测试启动")
    
    # 检查WAN状态
    if network_manager.wan_enabled:
        logger.info("多WAN功能已启用，正在检测状态...")
        
        # 测试所有WAN接口
        results = network_manager.test_all_wans()
        
        # 保存结果
        network_manager.save_results(results)
    else:
        logger.warning("多WAN功能未启用，无法进行测试")


if __name__ == "__main__":
    main()
