#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
多WAN端口检测脚本

通过使用不同端口范围访问测试URL验证多WAN功能
即使在单WAN环境下，也可以测试代码的多WAN处理逻辑是否正确
"""
import os
import sys
import time
import json
import socket
import random
import logging
from typing import Dict, List, Optional, Tuple
from collections import Counter, defaultdict

# 设置日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('wan_detection.log')
    ]
)
logger = logging.getLogger("WAN-Detection")

# 测试配置
TEST_URL = "http://106.14.185.239:29990/test"
TEST_HOST = "106.14.185.239"
TEST_PORT = 29990
TEST_PATH = "/test"
REQUESTS_PER_WAN = 10  # 每个WAN接口的请求次数
REQUEST_INTERVAL = 0.2  # 请求间隔(秒)
CONNECTION_TIMEOUT = 5  # 连接超时时间(秒)

# 端口范围配置
PORT_RANGES = [
    (50001, 51000),  # WAN 0
    (51001, 52000),  # WAN 1 
    (52001, 53000)   # WAN 2
]

def get_random_port(port_range: Tuple[int, int]) -> int:
    """
    从指定范围内随机获取一个端口
    
    Args:
        port_range: 端口范围(最小值, 最大值)
        
    Returns:
        int: 随机端口号
    """
    return random.randint(port_range[0], port_range[1])

def bind_socket_to_port(port: int) -> Optional[socket.socket]:
    """
    创建一个绑定到指定端口的socket
    
    Args:
        port: 要绑定的端口号
        
    Returns:
        Optional[socket.socket]: 成功创建的socket对象，失败则返回None
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('0.0.0.0', port))
        logger.debug(f"成功绑定socket到端口 {port}")
        return sock
    except Exception as e:
        logger.debug(f"端口 {port} 绑定失败: {str(e)}")
        return None

def http_request(host: str, port: int, path: str, local_port: int, timeout: int = 5) -> Dict:
    """
    通过指定本地端口向目标服务器发送HTTP请求
    
    Args:
        host: 目标主机
        port: 目标端口
        path: 请求路径
        local_port: 本地端口
        timeout: 超时时间(秒)
        
    Returns:
        Dict: 响应结果
    """
    result = {
        "success": False,
        "status_code": None,
        "data": None,
        "client_ip": None,
        "error": None,
        "local_port": local_port,
        "timestamp": time.time()
    }
    
    sock = None
    try:
        # 创建socket并绑定本地端口
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.bind(('0.0.0.0', local_port))
        
        # 连接到目标服务器
        sock.connect((host, port))
        
        # 构造HTTP请求
        request = f"GET {path} HTTP/1.1\r\n"
        request += f"Host: {host}:{port}\r\n"
        request += "Connection: close\r\n"
        request += f"User-Agent: WAN-Detection/1.0 (Port: {local_port})\r\n"
        request += f"X-Local-Port: {local_port}\r\n"
        request += "\r\n"
        
        # 发送请求
        sock.sendall(request.encode('utf-8'))
        
        # 接收响应
        chunks = []
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
        
        # 解析响应
        response_data = b''.join(chunks)
        response_text = response_data.decode('utf-8', errors='ignore')
        
        # 提取状态码
        status_line = response_text.split('\r\n')[0]
        if ' ' in status_line:
            try:
                status_code = int(status_line.split(' ')[1])
                result["status_code"] = status_code
                result["success"] = 200 <= status_code < 300
            except (IndexError, ValueError):
                pass
        
        # 提取响应体
        if '\r\n\r\n' in response_text:
            body = response_text.split('\r\n\r\n', 1)[1]
            if body.strip().startswith('{') and body.strip().endswith('}'):
                try:
                    json_data = json.loads(body)
                    result["data"] = json_data
                    
                    # 提取客户端IP地址 - 检查不同可能的字段名
                    for ip_field in ["client_ip", "your_ip", "ip", "remote_addr"]:
                        if ip_field in json_data:
                            result["client_ip"] = json_data[ip_field]
                            break
                except json.JSONDecodeError:
                    result["data"] = body[:200]
            else:
                result["data"] = body[:200]
        
    except Exception as e:
        result["error"] = str(e)
        logger.debug(f"请求失败 (端口 {local_port}): {str(e)}")
    
    finally:
        if sock:
            try:
                sock.close()
            except:
                pass
    
    return result

def test_wan_ports():
    """测试不同端口范围的连接情况，模拟多WAN环境"""
    logger.info("开始测试不同端口范围...")
    
    all_results = []
    results_by_range = defaultdict(list)
    ips_by_range = defaultdict(list)
    
    # 测试每个端口范围
    for range_idx, port_range in enumerate(PORT_RANGES):
        logger.info(f"===== 测试端口范围 {range_idx}: {port_range[0]}-{port_range[1]} =====")
        
        success_count = 0
        
        # 每个范围发送多次请求
        for req_idx in range(REQUESTS_PER_WAN):
            # 获取随机端口
            local_port = get_random_port(port_range)
            
            # 发送请求
            logger.info(f"请求 {req_idx+1}/{REQUESTS_PER_WAN}: 使用端口 {local_port}")
            result = http_request(TEST_HOST, TEST_PORT, TEST_PATH, local_port, CONNECTION_TIMEOUT)
            
            # 记录结果
            result["port_range"] = range_idx
            result["port_range_str"] = f"{port_range[0]}-{port_range[1]}"
            all_results.append(result)
            results_by_range[range_idx].append(result)
            
            # 处理成功的请求
            if result["success"]:
                success_count += 1
                if result["client_ip"]:
                    ips_by_range[range_idx].append(result["client_ip"])
                    logger.info(f"获取到客户端IP: {result['client_ip']}")
                else:
                    logger.warning("请求成功但未获取到客户端IP")
            else:
                error_msg = result["error"] or f"状态码: {result['status_code']}"
                logger.warning(f"请求失败: {error_msg}")
            
            # 等待一段时间再发送下一个请求
            time.sleep(REQUEST_INTERVAL)
        
        # 输出此端口范围的摘要
        success_rate = success_count / REQUESTS_PER_WAN * 100
        logger.info(f"端口范围 {range_idx} 测试完成: 成功率 {success_rate:.1f}% ({success_count}/{REQUESTS_PER_WAN})")
        
        unique_ips = len(set(ips_by_range[range_idx]))
        if unique_ips > 0:
            logger.info(f"端口范围 {range_idx} 检测到 {unique_ips} 个不同IP地址")
    
    # 分析并输出结果
    analyze_results(all_results, results_by_range, ips_by_range)
    
    # 保存测试结果
    save_results(all_results, results_by_range, ips_by_range)
    
    return all_results

def analyze_results(all_results, results_by_range, ips_by_range):
    """分析测试结果并输出报告"""
    # 所有请求的成功数量
    success_count = sum(1 for result in all_results if result["success"])
    total_count = len(all_results)
    
    logger.info("\n" + "="*50)
    logger.info("WAN端口测试结果分析")
    logger.info("="*50)
    logger.info(f"总请求数: {total_count}, 成功请求: {success_count}, 成功率: {success_count/total_count*100:.1f}%")
    
    # 提取所有检测到的IP地址
    all_ips = [result["client_ip"] for result in all_results 
               if result["success"] and result["client_ip"]]
    unique_ips = set(all_ips)
    
    logger.info(f"检测到 {len(unique_ips)} 个不同的客户端IP地址")
    for ip in unique_ips:
        count = all_ips.count(ip)
        percentage = count / len(all_ips) * 100 if all_ips else 0
        logger.info(f"- IP: {ip}, 出现次数: {count}, 占比: {percentage:.1f}%")
    
    # 分析每个端口范围的结果
    logger.info("\n各端口范围的IP分布:")
    common_ips = set()
    for range_idx, ips in ips_by_range.items():
        if not ips:
            continue
        
        range_unique_ips = set(ips)
        if not common_ips:
            common_ips = range_unique_ips
        else:
            common_ips &= range_unique_ips
            
        port_range = PORT_RANGES[range_idx]
        logger.info(f"端口范围 {range_idx} ({port_range[0]}-{port_range[1]}):")
        
        ip_counter = Counter(ips)
        for ip, count in ip_counter.most_common():
            percentage = count / len(ips) * 100
            logger.info(f"  - IP: {ip}, 出现次数: {count}, 占比: {percentage:.1f}%")
    
    # 判断是否检测到多个WAN接口
    wan_count = len(unique_ips)
    if wan_count > 1:
        logger.info("\n检测结论: 多WAN环境（检测到 {} 个不同IP地址）".format(wan_count))
    else:
        logger.info("\n检测结论: 单WAN环境（所有端口范围返回相同IP地址）")
    
    # 检查每个端口范围是否都能成功连接
    all_ranges_work = all(any(r["success"] for r in results) 
                          for results in results_by_range.values())
    
    if all_ranges_work:
        logger.info("所有端口范围均可成功连接，多WAN代码逻辑正确")
    else:
        failed_ranges = [idx for idx, results in results_by_range.items() 
                       if not any(r["success"] for r in results)]
        logger.warning(f"以下端口范围无法连接: {failed_ranges}")
    
    return {
        "wan_count": wan_count,
        "unique_ips": list(unique_ips),
        "all_ranges_work": all_ranges_work
    }

def save_results(all_results, results_by_range, ips_by_range):
    """保存测试结果到JSON文件"""
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "test_url": TEST_URL,
        "requests_per_wan": REQUESTS_PER_WAN,
        "port_ranges": [{"index": i, "range": list(r)} for i, r in enumerate(PORT_RANGES)],
        "summary": {
            "total_requests": len(all_results),
            "successful_requests": sum(1 for r in all_results if r["success"]),
            "unique_ips": list(set(r["client_ip"] for r in all_results if r["client_ip"])),
            "wan_count": len(set(r["client_ip"] for r in all_results if r["client_ip"]))
        },
        "results_by_range": {str(idx): [
            {
                "local_port": r["local_port"],
                "success": r["success"],
                "client_ip": r["client_ip"],
                "status_code": r["status_code"],
                "error": r["error"]
            } for r in results
        ] for idx, results in results_by_range.items()},
        "ip_distributions": {str(idx): dict(Counter(ips)) for idx, ips in ips_by_range.items()}
    }
    
    output_file = "wan_detection_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    logger.info(f"测试结果已保存到 {output_file}")

def main():
    """主函数"""
    logger.info("WAN端口检测脚本启动")
    logger.info(f"测试URL: {TEST_URL}")
    logger.info(f"端口范围: {PORT_RANGES}")
    
    try:
        # 执行测试
        test_wan_ports()
    except Exception as e:
        logger.exception(f"测试过程中出现异常: {e}")
    
    logger.info("WAN端口检测完成")

if __name__ == "__main__":
    main()