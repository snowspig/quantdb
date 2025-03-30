#!/usr/bin/env python3
"""
多WAN IP监控脚本 - 检测WANs的IP地址并在全部相同时发出警报（可能表示路由器故障）
"""
import os
import sys
import time
import json
import socket
import requests
import urllib.parse
from typing import Dict, List, Set, Optional
from loguru import logger

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入项目模块
from config import config_manager
from wan_manager.port_allocator import PortAllocator
from wan_manager.wan_monitor import wan_monitor

# 设置日志
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("wan_ip_monitor.log", level="DEBUG", rotation="1 MB")

# 服务器配置
IP_CHECK_URL = "http://106.14.185.239:29990/test"  # IP检测服务的URL
CHECK_INTERVAL = 300  # 每5分钟检查一次
ALERT_INTERVAL = 3600  # 触发警报后，1小时内不再重复警报

class WANIPMonitor:
    """
    WAN IP监控器，检查多个WAN是否具有相同IP（表示路由器可能故障）
    """
    
    def __init__(self):
        """
        初始化WAN IP监控器
        """
        # 检查WAN是否启用
        if not config_manager.is_wan_enabled():
            logger.error("多WAN功能未启用，请在配置文件中启用")
            sys.exit(1)
            
        # 初始化组件
        self.port_allocator = PortAllocator()
        
        # 打印端口范围信息
        for wan_idx, port_range in self.port_allocator.wan_port_ranges.items():
            start_port, end_port = port_range
            logger.info(f"WANIPMonitor: WAN {wan_idx} 端口范围 {start_port}-{end_port}")
            
        # 启动WAN监控
        wan_monitor.start_monitoring()
        time.sleep(2)  # 等待监控初始化
        
        # 获取可用WAN列表
        self.available_wans = self.port_allocator.get_available_wan_indices()
        logger.info(f"可用的WAN接口: {self.available_wans}")
        
        # 状态跟踪
        self.last_check_time = 0
        self.last_alert_time = 0
        self.wan_ips = {}
        self.alert_triggered = False
    
    def _bind_socket(self, sock: socket.socket, wan_idx: int) -> bool:
        """
        将套接字绑定到特定WAN端口
        
        Args:
            sock: 要绑定的套接字
            wan_idx: WAN接口索引
            
        Returns:
            是否绑定成功
        """
        try:
            logger.debug(f"开始为WAN {wan_idx} 分配端口...")
            port = self.port_allocator.allocate_port(wan_idx)
            if port:
                logger.debug(f"为WAN {wan_idx} 分配到端口 {port}，开始绑定")
                sock.bind(('0.0.0.0', port))
                logger.debug(f"套接字成功绑定到WAN {wan_idx}, 端口 {port}")
                return True
            else:
                logger.warning(f"无法为WAN {wan_idx} 分配端口")
                return False
        except Exception as e:
            logger.error(f"绑定套接字到WAN {wan_idx} 失败: {str(e)}")
            return False
    
    def request_via_wan(self, wan_idx: int) -> Dict:
        """
        通过指定WAN接口发送请求以获取公网IP
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            请求结果字典
        """
        logger.info(f"通过WAN {wan_idx} 发送请求到 {IP_CHECK_URL}")
        
        try:
            # 从URL解析主机和端口
            parsed_url = urllib.parse.urlparse(IP_CHECK_URL)
            host = parsed_url.hostname
            port = parsed_url.port or 80
            
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
                
                # 连接到服务器
                start_time = time.time()
                sock.connect((host, port))
                
                # 构建HTTP请求
                request = f"GET {parsed_url.path}?wan_idx={wan_idx}&client_time={time.time()} HTTP/1.1\r\n"
                request += f"Host: {host}:{port}\r\n"
                request += "Connection: close\r\n\r\n"
                
                # 发送请求
                sock.sendall(request.encode())
                
                # 接收响应
                response = b""
                while True:
                    data = sock.recv(4096)
                    if not data:
                        break
                    response += data
                
                # 计算时间和解析响应
                elapsed = time.time() - start_time
                response_str = response.decode('utf-8', errors='ignore')
                
                # 尝试提取JSON部分
                json_start = response_str.find('{')
                if json_start != -1:
                    result = json.loads(response_str[json_start:])
                    ip_address = result.get('your_ip', '')
                    logger.info(f"WAN {wan_idx} 请求成功, 服务器看到的IP: {ip_address}")
                    
                    # 保存结果
                    result_data = {
                        "wan_idx": wan_idx,
                        "success": True,
                        "ip_address": ip_address,
                        "elapsed": elapsed,
                        "status_code": 200,
                        "timestamp": time.time()
                    }
                    return result_data
                else:
                    raise Exception("无法解析JSON响应")
                    
            finally:
                sock.close()
                
        except Exception as e:
            logger.error(f"WAN {wan_idx} 请求异常: {str(e)}")
            return {
                "wan_idx": wan_idx,
                "success": False,
                "error": str(e),
                "timestamp": time.time()
            }
    
    def check_wan_ips(self) -> Dict:
        """
        检查所有WAN接口的IP地址并返回结果
        
        Returns:
            每个WAN的检查结果
        """
        logger.info("开始检查所有WAN接口的IP地址...")
        
        results = {}
        for wan_idx in self.available_wans:
            result = self.request_via_wan(wan_idx)
            if result["success"]:
                results[wan_idx] = result
            time.sleep(1)  # 防止请求太快
            
        return results
    
    def analyze_ip_results(self, results: Dict) -> Dict:
        """
        分析IP检查结果，检测是否有问题
        
        Args:
            results: 每个WAN的IP检查结果
            
        Returns:
            分析结果
        """
        if not results:
            return {
                "status": "error",
                "message": "没有收到有效的IP检查结果",
                "timestamp": time.time()
            }
        
        # 提取IP地址
        ip_by_wan = {}
        for wan_idx, result in results.items():
            if result.get("success") and "ip_address" in result:
                ip_by_wan[wan_idx] = result["ip_address"]
        
        # 统计唯一IP数量
        unique_ips = set(ip_by_wan.values())
        ip_counts = {}
        for ip in unique_ips:
            ip_counts[ip] = list(ip_by_wan.values()).count(ip)
            
        # 检查是否所有WAN都返回相同IP
        all_same_ip = len(unique_ips) == 1 and len(ip_by_wan) > 1
        
        analysis = {
            "status": "alert" if all_same_ip else "normal",
            "message": "所有WAN接口返回相同IP地址！可能是路由器故障。" if all_same_ip else "WAN接口IP地址正常",
            "unique_ip_count": len(unique_ips),
            "wan_count": len(ip_by_wan),
            "ip_by_wan": ip_by_wan,
            "timestamp": time.time()
        }
        
        logger.info(f"WAN IP分析结果: {analysis['status']} - {analysis['message']}")
        return analysis
    
    def send_alert(self, analysis: Dict):
        """
        根据设置发送警报通知
        
        Args:
            analysis: IP分析结果
        """
        # 检查是否需要发送警报（避免过度警报）
        current_time = time.time()
        if current_time - self.last_alert_time < ALERT_INTERVAL:
            logger.info("警报间隔内，跳过此次警报")
            return
            
        # 记录此次警报时间
        self.last_alert_time = current_time
        
        message = f"""
警报：多WAN IP监控检测到问题！

时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))}
问题: {analysis['message']}

详细信息:
- 检测到的唯一IP数量: {analysis['unique_ip_count']}
- WAN接口数量: {analysis['wan_count']}
- 每个WAN接口的IP:
"""
        
        for wan_idx, ip in analysis['ip_by_wan'].items():
            message += f"  WAN {wan_idx}: {ip}\n"
            
        logger.warning(f"发送警报: \n{message}")
        
        # 将警报保存到文件
        with open("wan_ip_alert.log", "a") as f:
            f.write(message + "\n" + "-"*50 + "\n")
            
        # 这里可以添加邮件发送、短信或其他通知方式
        # 示例：输出到控制台
        print("\n" + "!"*50)
        print(message)
        print("!"*50 + "\n")
    
    def run_monitoring(self):
        """
        启动WAN IP监控循环
        """
        logger.info("WAN IP监控服务启动...")
        
        try:
            while True:
                current_time = time.time()
                
                # 检查是否到达检查时间间隔
                if current_time - self.last_check_time >= CHECK_INTERVAL:
                    self.last_check_time = current_time
                    
                    # 检查WAN IP
                    results = self.check_wan_ips()
                    
                    # 分析结果
                    analysis = self.analyze_ip_results(results)
                    
                    # 如果检测到问题，发送警报
                    if analysis["status"] == "alert":
                        self.send_alert(analysis)
                        
                    # 保存检查结果
                    with open("wan_ip_checks.json", "a") as f:
                        f.write(json.dumps({
                            "time": time.strftime('%Y-%m-%d %H:%M:%S'),
                            "results": results,
                            "analysis": analysis
                        }, default=str) + "\n")
                
                # 休眠一段时间
                time.sleep(10)
                
        except KeyboardInterrupt:
            logger.info("WAN IP监控服务被用户中断")
        except Exception as e:
            logger.error(f"WAN IP监控服务发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            logger.info("WAN IP监控服务已停止")


def main():
    """
    主函数 - 启动WAN IP监控服务
    """
    logger.info("WAN IP监控脚本启动")
    
    monitor = WANIPMonitor()
    
    # 执行单次检查并验证结果
    results = monitor.check_wan_ips()
    analysis = monitor.analyze_ip_results(results)
    
    if analysis["status"] == "alert":
        monitor.send_alert(analysis)
    
    # 启动持续监控
    monitor.run_monitoring()


if __name__ == "__main__":
    main()
