#!/usr/bin/env python3
"""
WAN IP测试工具 - 用于测试多WAN IP监控功能，可以模拟多个WAN接口返回相同IP的情况
"""
import os
import sys
import time
import json
import argparse
from loguru import logger

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入我们创建的WAN IP监控器
from wan_ip_monitor import WANIPMonitor

# 设置日志
logger.remove()
logger.add(sys.stderr, level="INFO")

def simulate_wan_check(simulation_mode="normal"):
    """
    模拟WAN IP检查
    
    Args:
        simulation_mode: 模拟模式
            - "normal": 正常模式，每个WAN有不同IP
            - "same_ip": 所有WAN返回相同IP（模拟路由器故障）
            - "partial_same": 部分WAN返回相同IP
    """
    logger.info(f"WAN IP测试工具 - 模拟模式: {simulation_mode}")
    
    # 初始化监控器
    monitor = WANIPMonitor()
    
    # 保存原始方法
    original_request_method = monitor.request_via_wan
    
    # 模拟IP地址
    simulated_ips = {
        "normal": {
            0: "123.45.67.89",
            1: "98.76.54.32",
            2: "111.222.333.444"
        },
        "same_ip": {
            0: "123.45.67.89",
            1: "123.45.67.89",
            2: "123.45.67.89"
        },
        "partial_same": {
            0: "123.45.67.89",
            1: "123.45.67.89",
            2: "111.222.333.444"
        }
    }
    
    # 定义模拟的请求方法
    def mocked_request_via_wan(wan_idx):
        time.sleep(0.5)  # 模拟网络延迟
        ip = simulated_ips.get(simulation_mode, {}).get(wan_idx, f"192.168.1.{wan_idx}")
        logger.info(f"模拟 WAN {wan_idx} 请求, 返回IP: {ip}")
        
        return {
            "wan_idx": wan_idx,
            "success": True,
            "ip_address": ip,
            "elapsed": 0.5,
            "status_code": 200,
            "timestamp": time.time()
        }
    
    try:
        # 替换方法
        monitor.request_via_wan = mocked_request_via_wan
        
        # 执行检查
        results = monitor.check_wan_ips()
        analysis = monitor.analyze_ip_results(results)
        
        # 输出分析结果
        print("\n====== 测试结果 ======")
        print(f"状态: {analysis['status']}")
        print(f"消息: {analysis['message']}")
        print(f"唯一IP数量: {analysis['unique_ip_count']}")
        print(f"WAN接口数量: {analysis['wan_count']}")
        print("\nWAN接口IP:")
        for wan_idx, ip in analysis.get('ip_by_wan', {}).items():
            print(f"  WAN {wan_idx}: {ip}")
        print("======================\n")
        
        # 如果状态为alert，发送测试警报
        if analysis["status"] == "alert":
            print("\n触发警报测试:\n")
            monitor.send_alert(analysis)
        
        return analysis
        
    finally:
        # 恢复原始方法
        monitor.request_via_wan = original_request_method

def main():
    # 创建命令行解析器
    parser = argparse.ArgumentParser(description="WAN IP监控测试工具")
    parser.add_argument(
        "--mode", 
        choices=["normal", "same_ip", "partial_same"], 
        default="normal",
        help="测试模式: normal=正常不同IP, same_ip=全部相同IP, partial_same=部分相同IP"
    )
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 运行测试
    simulate_wan_check(args.mode)


if __name__ == "__main__":
    main()
