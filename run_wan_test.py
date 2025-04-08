#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
执行WAN测试，对比wan_test_client.py和network_manager.py
"""
import os
import sys
import time
import json
import subprocess
import logging

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def run_command(command):
    """运行命令并返回输出"""
    logging.info(f"执行命令: {command}")
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=True,
            capture_output=True,
            text=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        logging.error(f"命令执行失败: {e}")
        logging.error(f"错误输出: {e.stderr}")
        return e.stderr

def main():
    # 当前目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    logging.info(f"当前目录: {current_dir}")
    
    # 运行wan_test_client.py
    logging.info("=== 运行 wan_test_client.py ===")
    wan_test_output = run_command(f"cd {current_dir} && python wan_test_client.py")
    
    # 保存原始输出
    with open("wan_test_client_output.log", "w") as f:
        f.write(wan_test_output)
    logging.info(f"wan_test_client.py输出已保存到wan_test_client_output.log")
    
    # 等待一段时间
    time.sleep(3)
    
    # 运行network_manager.py
    logging.info("\n=== 运行 network_manager.py ===")
    network_manager_output = run_command(f"cd {current_dir} && python network_manager.py")
    
    # 保存原始输出
    with open("network_manager_output.log", "w") as f:
        f.write(network_manager_output)
    logging.info(f"network_manager.py输出已保存到network_manager_output.log")
    
    # 加载测试结果进行比较
    try:
        with open("wan_test_results.json", "r") as f:
            wan_test_results = json.load(f)
        logging.info(f"wan_test_client.py结果: {len(wan_test_results)}个测试")
        
        success_count = sum(1 for r in wan_test_results if r.get("success", False))
        unique_ips = set()
        for result in wan_test_results:
            if result.get("success") and result.get("server_response", {}).get("your_ip"):
                unique_ips.add(result.get("server_response", {}).get("your_ip"))
                
        logging.info(f"wan_test_client.py成功测试: {success_count}/{len(wan_test_results)}")
        logging.info(f"wan_test_client.py检测到的不同IP: {len(unique_ips)}")
    except Exception as e:
        logging.error(f"无法加载wan_test_results.json: {e}")
    
    try:
        with open("network_manager_results.json", "r") as f:
            nm_results = json.load(f)
        logging.info(f"network_manager.py结果: {len(nm_results.get('results', []))}个测试")
        
        success_count = sum(1 for r in nm_results.get("results", []) if r.get("success", False))
        unique_ips = set(nm_results.get("unique_ips", []))
                
        logging.info(f"network_manager.py成功测试: {success_count}/{len(nm_results.get('results', []))}")
        logging.info(f"network_manager.py检测到的不同IP: {len(unique_ips)}")
        logging.info(f"多WAN状态: {nm_results.get('message', '未知')}")
    except Exception as e:
        logging.error(f"无法加载network_manager_results.json: {e}")
    
    # 比较结果
    logging.info("\n=== 比较结果 ===")
    logging.info("请查看两个日志文件和JSON结果文件获取详细信息")
    logging.info("测试完成！")

if __name__ == "__main__":
    main()
