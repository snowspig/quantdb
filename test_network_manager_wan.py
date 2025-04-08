#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
网络管理器多WAN测试
通过访问测试URL验证多WAN接口功能
"""
import os
import sys
import time
import json
import logging
from typing import Dict, List
from collections import Counter

# 导入配置和网络模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.network_manager import network_manager
from config.config_manager import config_manager

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("NetworkManagerTest")

# 测试配置
TEST_URL = "http://106.14.185.239:29990/test"
REQUESTS_PER_WAN = 10
TEST_TIMEOUT = 5


def test_multi_wan_connectivity():
    """
    测试多WAN口连接功能
    使用不同WAN接口多次请求测试URL，并分析返回的客户端IP
    """
    # 检查WAN配置
    wan_config = config_manager.get_wan_config()
    if not wan_config.get("enabled", False):
        logger.error("WAN功能未启用，请先启用WAN配置")
        return False

    # 获取WAN接口状态
    interfaces_status = network_manager.get_interfaces_status()
    if not interfaces_status["active_interfaces"]:
        logger.error("没有可用的WAN接口")
        return False
        
    logger.info(f"开始测试 {interfaces_status['active_interfaces']} 个活跃WAN接口")
    
    # 测试每个接口
    results = []
    ip_addresses = []
    
    # 刷新网络接口状态
    network_manager.refresh_interfaces()
    
    # 创建不同的会话，每个会话对应一个WAN接口
    sessions = []
    for i, interface in enumerate(interfaces_status["interfaces"]):
        if interface["active"]:
            session_id = network_manager.create_session(
                name=f"wan_{i}_test",
                interface=interface  # 使用特定接口
            )
            sessions.append((i, session_id, interface))
    
    # 使用每个会话发送多次请求
    for wan_idx, session_id, interface in sessions:
        logger.info(f"测试WAN接口 {wan_idx} ({interface['name']}) 源IP: {interface['source_ip']}")
        
        for i in range(REQUESTS_PER_WAN):
            try:
                # 使用会话发送请求
                response = network_manager.request(
                    "get", 
                    TEST_URL, 
                    session_id=session_id,
                    timeout=TEST_TIMEOUT
                )
                
                if response and response.status_code == 200:
                    # 解析响应内容
                    try:
                        data = response.json()
                        client_ip = data.get("client_ip", "未知")
                        ip_addresses.append(client_ip)
                        
                        result = {
                            "wan_index": wan_idx,
                            "interface_name": interface["name"],
                            "source_ip": interface["source_ip"],
                            "client_ip": client_ip,
                            "timestamp": time.time(),
                            "success": True
                        }
                        results.append(result)
                        
                        logger.info(f"请求 {i+1}/{REQUESTS_PER_WAN}: WAN {wan_idx} 获得客户端IP: {client_ip}")
                    except Exception as e:
                        logger.error(f"解析响应失败: {str(e)}")
                        
                else:
                    logger.error(f"请求失败，状态码: {response.status_code if response else 'None'}")
            
            except Exception as e:
                logger.error(f"请求异常: {str(e)}")
            
            # 暂停一下，避免请求过于频繁
            time.sleep(0.5)
    
    # 关闭所有会话
    for _, session_id, _ in sessions:
        network_manager.close_session(session_id)
    
    # 分析结果
    analyze_results(results, ip_addresses)
    
    # 保存测试结果
    save_results(results)
    
    return True


def analyze_results(results: List[Dict], ip_addresses: List[str]):
    """分析测试结果"""
    if not results:
        logger.warning("没有测试结果可分析")
        return
    
    # 统计每个IP地址的出现次数
    ip_counter = Counter(ip_addresses)
    unique_ips = len(ip_counter)
    
    logger.info("\n=== 多WAN测试分析结果 ===")
    logger.info(f"共发送 {len(results)} 个成功请求")
    
    if unique_ips > 1:
        logger.info(f"检测到 {unique_ips} 个不同的源IP地址，表明多WAN接口工作正常")
        for ip, count in ip_counter.items():
            logger.info(f"- IP地址: {ip}, 请求次数: {count}")
    elif unique_ips == 1:
        ip, count = list(ip_counter.items())[0]
        logger.info(f"只检测到1个源IP地址 ({ip})，请求次数: {count}")
        logger.info("这表明可能只有单一WAN接口在工作，或所有WAN接口共享同一个IP地址")
    else:
        logger.error("无法从响应中提取客户端IP信息")
    
    # 按接口分析成功率
    wan_results = {}
    for result in results:
        wan_idx = result["wan_index"]
        if wan_idx not in wan_results:
            wan_results[wan_idx] = {"total": 0, "success": 0}
        
        wan_results[wan_idx]["total"] += 1
        if result["success"]:
            wan_results[wan_idx]["success"] += 1
    
    logger.info("\n各WAN接口请求结果:")
    for wan_idx, stats in wan_results.items():
        success_rate = stats["success"] / stats["total"] * 100 if stats["total"] > 0 else 0
        logger.info(f"WAN {wan_idx}: 成功率 {success_rate:.1f}% ({stats['success']}/{stats['total']})")


def save_results(results: List[Dict]):
    """保存测试结果到文件"""
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "test_url": TEST_URL,
        "requests_per_wan": REQUESTS_PER_WAN,
        "total_requests": len(results),
        "results": results,
        "summary": {
            "unique_ips": len(set(r["client_ip"] for r in results if "client_ip" in r))
        }
    }
    
    with open("network_manager_wan_test_results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    logger.info("测试结果已保存到 network_manager_wan_test_results.json")


def main():
    """主函数"""
    logger.info("网络管理器多WAN测试启动")
    
    try:
        # 测试多WAN连接
        test_multi_wan_connectivity()
    except Exception as e:
        logger.exception(f"测试过程中出现异常: {str(e)}")
    
    logger.info("网络管理器多WAN测试完成")


if __name__ == "__main__":
    main()