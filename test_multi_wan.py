#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
多WAN功能测试脚本
"""
import os
import sys
import time
import json
import socket
import random
import requests
from loguru import logger

# 导入项目配置和网络模块
from config.config_manager import config_manager
from wan_manager.port_allocator import PortAllocator

# 尝试导入wan_monitor，如果不存在则忽略
try:
    from wan_manager.wan_monitor import wan_monitor
except ImportError:
    wan_monitor = None
    logger.warning("WAN监控模块未找到")

# 设置日志
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("test_multi_wan.log", level="DEBUG", rotation="1 MB")

# 测试服务器配置
SERVER_URL = "http://106.14.185.239:29990/test"

def test_wan_configuration():
    """测试WAN配置"""
    # 确认WAN是否启用
    wan_enabled = config_manager.is_wan_enabled()
    wan_config = config_manager.get_wan_config()
    
    logger.info(f"WAN配置状态: {'启用' if wan_enabled else '未启用'}")
    logger.info(f"WAN配置详情: {json.dumps(wan_config, ensure_ascii=False, indent=2)}")
    
    # 检查接口配置
    interfaces = wan_config.get('interfaces', [])
    logger.info(f"配置的接口数量: {len(interfaces)}")
    
    for idx, interface in enumerate(interfaces):
        logger.info(f"接口 {idx+1}: {interface['name']} - {'启用' if interface.get('enabled', False) else '禁用'} - 源IP: {interface.get('source_ip', 'N/A')}")
    
    return wan_enabled, interfaces

def test_wan_connectivity():
    """测试WAN连接性"""
    try:
        # 使用默认连接测试服务器连通性
        response = requests.get(SERVER_URL, timeout=5)
        if response.status_code == 200:
            logger.info(f"服务器连接成功: {SERVER_URL}")
            logger.info(f"服务器返回: {response.text[:100]}...")
            return True
        else:
            logger.warning(f"服务器连接失败，状态码: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"服务器连接异常: {str(e)}")
        return False

def bind_socket_to_port(port):
    """测试端口绑定"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('0.0.0.0', port))
        logger.info(f"成功绑定到端口 {port}")
        sock.close()
        return True
    except Exception as e:
        logger.error(f"端口 {port} 绑定失败: {str(e)}")
        return False

def test_port_allocator():
    """测试端口分配器"""
    try:
        logger.info("初始化端口分配器...")
        allocator = PortAllocator()
        
        # 获取可用WAN接口
        available_wans = allocator.get_available_wan_indices()
        logger.info(f"可用WAN接口: {available_wans}")
        
        # 测试端口分配
        for wan_idx in available_wans:
            port = allocator.allocate_port(wan_idx)
            if port:
                logger.info(f"WAN {wan_idx} 分配到端口 {port}")
                # 测试绑定
                bind_socket_to_port(port)
                # 释放端口
                allocator.release_port(wan_idx, port)
            else:
                logger.warning(f"WAN {wan_idx} 无法分配端口")
                
        return len(available_wans) > 0
    except Exception as e:
        logger.error(f"端口分配器测试失败: {str(e)}")
        return False

def test_socket_connection(wan_idx):
    """测试通过特定WAN接口连接"""
    try:
        # 分配端口
        allocator = PortAllocator()
        port = allocator.allocate_port(wan_idx)
        
        if not port:
            logger.error(f"无法为WAN {wan_idx} 分配端口")
            return False
            
        logger.info(f"为WAN {wan_idx} 分配端口 {port}")
        
        # 创建套接字并绑定
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        
        try:
            # 绑定到分配的端口
            sock.bind(('0.0.0.0', port))
            
            # 解析测试服务器地址
            host = "106.14.185.239"
            port = 29990
            
            # 连接测试
            logger.info(f"通过WAN {wan_idx} 连接到 {host}:{port}")
            sock.connect((host, port))
            
            # 发送简单HTTP请求
            request = f"GET /test?wan_idx={wan_idx}&timestamp={time.time()} HTTP/1.1\r\n"
            request += f"Host: {host}:{port}\r\n"
            request += "Connection: close\r\n\r\n"
            
            sock.sendall(request.encode())
            
            # 接收响应
            response = b""
            while True:
                data = sock.recv(4096)
                if not data:
                    break
                response += data
                
            response_str = response.decode('utf-8', errors='ignore')
            logger.info(f"WAN {wan_idx} 连接成功，收到响应: {response_str[:100]}...")
            return True
            
        except Exception as e:
            logger.error(f"WAN {wan_idx} 连接测试失败: {str(e)}")
            return False
        finally:
            sock.close()
            allocator.release_port(wan_idx, port)
            
    except Exception as e:
        logger.error(f"WAN {wan_idx} 测试过程失败: {str(e)}")
        return False

def run_all_tests():
    """运行所有测试"""
    results = {}
    
    # 测试WAN配置
    logger.info("=== 测试WAN配置 ===")
    wan_enabled, interfaces = test_wan_configuration()
    results['wan_config'] = {
        'enabled': wan_enabled,
        'interfaces_count': len(interfaces)
    }
    
    # 测试服务器连通性
    logger.info("\n=== 测试服务器连通性 ===")
    server_connectivity = test_wan_connectivity()
    results['server_connectivity'] = server_connectivity
    
    # 测试端口分配器
    logger.info("\n=== 测试端口分配器 ===")
    port_allocator_working = test_port_allocator()
    results['port_allocator'] = port_allocator_working
    
    # 如果配置了WAN，测试每个接口
    if wan_enabled and port_allocator_working:
        logger.info("\n=== 测试每个WAN接口连接 ===")
        wan_results = {}
        
        allocator = PortAllocator()
        available_wans = allocator.get_available_wan_indices()
        
        for wan_idx in available_wans:
            logger.info(f"\n-- 测试WAN接口 {wan_idx} --")
            success = test_socket_connection(wan_idx)
            wan_results[f'wan_{wan_idx}'] = success
            time.sleep(1)  # 避免请求过快
        
        results['wan_connections'] = wan_results
    
    # 打印结果摘要
    logger.info("\n=== 测试结果摘要 ===")
    for key, value in results.items():
        logger.info(f"{key}: {value}")
    
    return results

def main():
    """主函数"""
    logger.info("多WAN功能测试脚本启动")
    
    try:
        results = run_all_tests()
        
        # 保存结果
        with open("multi_wan_test_results.json", 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        logger.info("测试结果已保存到 multi_wan_test_results.json")
        
    except Exception as e:
        logger.error(f"测试过程出现异常: {str(e)}")
    
    logger.info("多WAN功能测试完成")

if __name__ == "__main__":
    main()