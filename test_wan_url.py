#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
多WAN URL测试脚本 - 通过测试URL验证多WAN接口功能

这个脚本通过访问测试URL并使用不同的本地端口范围，
验证系统是否成功使用多个WAN接口进行访问。
"""
import sys
import time
import json
import socket
import random
import requests
import ipaddress
from collections import Counter, defaultdict
from loguru import logger
from typing import Dict, List, Tuple, Optional, Set

# 导入项目配置和网络模块
from config.config_manager import config_manager
from wan_manager.port_allocator import PortAllocator

# 设置日志
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("test_wan_url.log", level="DEBUG", rotation="1 MB")

# 测试服务器配置
TEST_URL = "http://106.14.185.239:29990/test"
NUM_REQUESTS = 30  # 每个WAN接口的请求次数
REQUEST_INTERVAL = 0.5  # 请求间隔(秒)


def get_local_ip() -> str:
    """获取本地IP地址"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception as e:
        logger.error(f"获取本地IP地址失败: {str(e)}")
        return "127.0.0.1"


def setup_socket_with_port(port: int) -> Optional[socket.socket]:
    """
    设置绑定特定端口的socket
    
    Args:
        port: 要绑定的本地端口
        
    Returns:
        成功绑定的socket对象，或None表示绑定失败
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('0.0.0.0', port))
        logger.debug(f"成功绑定到端口 {port}")
        return sock
    except Exception as e:
        logger.error(f"端口 {port} 绑定失败: {str(e)}")
        return None


def make_request_with_port(url: str, port: int, timeout: int = 10) -> Dict:
    """
    使用指定的本地端口发送HTTP请求
    
    Args:
        url: 目标URL
        port: 本地端口
        timeout: 请求超时时间(秒)
        
    Returns:
        包含响应信息的字典
    """
    result = {
        "port": port,
        "success": False,
        "error": None,
        "status_code": None,
        "data": None,
        "source_ip": None,
        "timestamp": time.time()
    }
    
    try:
        # 创建 socket 并绑定到指定端口
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('0.0.0.0', port))
        
        # 解析 URL 和提取主机名/端口
        if url.startswith('http://'):
            host = url[7:].split('/')[0]
        elif url.startswith('https://'):
            host = url[8:].split('/')[0]
        else:
            host = url.split('/')[0]
        
        # 分离主机名和端口
        if ':' in host:
            host, port_str = host.split(':')
            target_port = int(port_str)
        else:
            target_port = 80 if url.startswith('http://') else 443
            
        # 连接到目标服务器
        logger.debug(f"连接到 {host}:{target_port} 使用本地端口 {port}")
        sock.connect((host, target_port))
        
        # 构造HTTP请求
        path = '/' + '/'.join(url.split('/')[3:]) if len(url.split('/')) > 3 else '/'
        request = f"GET {path} HTTP/1.1\r\n"
        request += f"Host: {host}:{target_port}\r\n"
        request += "Connection: close\r\n"
        request += f"User-Agent: MultiWANTester/1.0 (Port: {port})\r\n"
        request += f"X-Local-Port: {port}\r\n"
        request += "\r\n"
        
        # 发送请求
        sock.sendall(request.encode('utf-8'))
        
        # 接收响应
        response_data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response_data += chunk
            
        sock.close()
        
        # 解析响应
        response_text = response_data.decode('utf-8', errors='ignore')
        
        # 提取状态码
        status_line = response_text.split('\r\n')[0]
        status_code = int(status_line.split(' ')[1]) if len(status_line.split(' ')) > 1 else 0
        
        result["success"] = True
        result["status_code"] = status_code
        
        # 提取响应体（简单处理）
        if '\r\n\r\n' in response_text:
            body = response_text.split('\r\n\r\n', 1)[1]
            try:
                # 尝试解析为JSON
                if body.strip().startswith('{') and body.strip().endswith('}'): 
                    import json
                    result["data"] = json.loads(body)
                    if "client_ip" in result["data"]:
                        result["source_ip"] = result["data"]["client_ip"]
                else:
                    result["data"] = body[:200]
            except:
                result["data"] = body[:200]
        else:
            result["data"] = response_text[:200]
            
    except Exception as e:
        result["error"] = str(e)
        
    return result


def test_wan_url_access():
    """测试通过不同WAN接口访问URL"""
    # 获取WAN配置
    wan_config = config_manager.get_wan_config()
    
    if not wan_config.get("enabled", False):
        logger.error("WAN功能未启用，请先启用WAN配置")
        return False
    
    # 初始化端口分配器
    port_allocator = PortAllocator()
    available_wans = port_allocator.get_available_wan_indices()
    
    if not available_wans:
        logger.error("没有可用的WAN接口")
        return False
    
    logger.info(f"开始测试 {len(available_wans)} 个WAN接口...")
    results_by_wan = defaultdict(list)
    
    # 对每个WAN接口进行测试
    for wan_idx in available_wans:
        logger.info(f"=== 测试WAN接口 {wan_idx} ===")
        
        # 进行多次请求
        for i in range(NUM_REQUESTS):
            # 分配端口
            port = port_allocator.allocate_port(wan_idx)
            
            if not port:
                logger.warning(f"WAN {wan_idx} 无法分配端口，跳过此次请求")
                continue
                
            logger.info(f"请求 {i+1}/{NUM_REQUESTS}: 使用WAN {wan_idx}, 端口 {port}")
            
            # 发送请求并记录结果
            result = make_request_with_port(TEST_URL, port)
            
            # 释放端口
            port_allocator.release_port(wan_idx, port)
            
            if result["success"]:
                logger.info(f"请求成功: 状态码 {result['status_code']}")
                results_by_wan[wan_idx].append(result)
            else:
                logger.error(f"请求失败: {result['error']}")
            
            # 暂停一小段时间，避免请求过于频繁
            time.sleep(REQUEST_INTERVAL)
    
    # 分析结果
    analyze_results(results_by_wan)
    
    # 保存测试结果
    save_results(results_by_wan)
    
    return True


def analyze_results(results_by_wan):
    """分析测试结果，查找使用了多个WAN接口的证据"""
    # 合并所有结果
    all_results = []
    for wan_idx, results in results_by_wan.items():
        all_results.extend(results)
    
    if not all_results:
        logger.warning("没有成功的请求结果，无法分析")
        return
    
    # 统计每个源IP地址的请求数量
    ip_counter = Counter()
    for result in all_results:
        data = result.get("data", {})
        if isinstance(data, dict) and "client_ip" in data:
            ip_counter[data["client_ip"]] += 1
        elif isinstance(data, str) and "client_ip" in data:
            # 尝试从字符串中提取IP
            try:
                # 简单提取，实际可能需要更复杂的解析
                if "client_ip" in data:
                    parts = data.split("client_ip")
                    if len(parts) > 1:
                        ip_part = parts[1].split('"')[2]
                        if ip_part:
                            ip_counter[ip_part] += 1
            except:
                pass
    
    # 输出分析结果
    logger.info("\n=== 多WAN测试分析结果 ===")
    logger.info(f"共发送 {len(all_results)} 个成功请求")
    
    if len(ip_counter) > 1:
        logger.info(f"检测到 {len(ip_counter)} 个不同的源IP地址，表明多WAN接口工作正常")
        for ip, count in ip_counter.items():
            logger.info(f"- IP地址: {ip}, 请求次数: {count}")
    elif len(ip_counter) == 1:
        ip, count = list(ip_counter.items())[0]
        logger.warning(f"只检测到1个源IP地址 ({ip})，请求次数: {count}")
        logger.warning("这表明可能只有单一WAN接口在工作，或所有WAN接口共享同一个IP地址")
    else:
        logger.error("无法从响应中提取客户端IP信息")
    
    # 按照WAN接口分析
    logger.info("\n各WAN接口请求结果:")
    for wan_idx, results in results_by_wan.items():
        success_rate = len(results) / NUM_REQUESTS * 100 if results else 0
        logger.info(f"WAN {wan_idx}: 成功率 {success_rate:.1f}% ({len(results)}/{NUM_REQUESTS})")


def save_results(results_by_wan):
    """保存测试结果到文件"""
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "test_url": TEST_URL,
        "requests_per_wan": NUM_REQUESTS,
        "results_by_wan": {str(k): v for k, v in results_by_wan.items()}
    }
    
    # 简化输出，避免保存过大的响应内容
    for wan_idx, results in output["results_by_wan"].items():
        for result in results:
            if isinstance(result["data"], dict) and "client_ip" in result["data"]:
                result["client_ip"] = result["data"]["client_ip"]
            result["data"] = str(result["data"])[:100] + "..." if result["data"] else None
    
    with open("wan_url_test_results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    logger.info("测试结果已保存到 wan_url_test_results.json")


def main():
    """主函数"""
    logger.info("多WAN URL访问测试脚本启动")
    
    try:
        # 执行测试
        test_wan_url_access()
        
    except Exception as e:
        logger.error(f"测试过程出现异常: {str(e)}")
    
    logger.info("多WAN URL访问测试完成")


if __name__ == "__main__":
    main()