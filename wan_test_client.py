"""
WAN测试客户端 - 测试多WAN绑定功能
"""
import os
import sys
import time
import json
import socket
import requests
import random
from loguru import logger
import urllib.parse

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入项目模块
from config import config_manager
from wan_manager.port_allocator import PortAllocator
from wan_manager.load_balancer import load_balancer
from wan_manager.wan_monitor import wan_monitor

# 设置日志
logger.remove()
logger.add(sys.stderr, level="DEBUG")
logger.add("wan_test_client.log", level="DEBUG", rotation="1 MB")

# 服务器配置
SERVER_URL = "http://106.14.185.239:29990/test"

class WANTester:
    """WAN功能测试器"""
    
    def __init__(self):
        """初始化WAN测试器"""
        # 检查WAN是否启用
        if not config_manager.is_wan_enabled():
            logger.error("多WAN功能未启用,请在配置文件中启用")
            sys.exit(1)
            
        # 初始化组件
        self.port_allocator = PortAllocator()
        
        # 打印端口范围信息
        print(f"WANTester中的端口配置: {self.port_allocator.wan_port_ranges}")
        for wan_idx, port_range in self.port_allocator.wan_port_ranges.items():
            start_port, end_port = port_range
            print(f"WANTester: WAN {wan_idx} 端口范围 {start_port}-{end_port}")
            
        # 启动WAN监控
        wan_monitor.start_monitoring()
        time.sleep(2)  # 等待监控初始化
        
        # 获取可用WAN列表
        self.available_wans = self.port_allocator.get_available_wan_indices()
        logger.info(f"可用的WAN接口: {self.available_wans}")
        
        # 测试结果
        self.results = []
    
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
    
    def request_via_wan(self, wan_idx: int) -> dict:
        """通过指定WAN接口发送请求"""
        logger.info(f"通过WAN {wan_idx} 发送请求到 {SERVER_URL}")
        
        try:
            # 从URL解析主机和端口
            parsed_url = urllib.parse.urlparse(SERVER_URL)
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
                    logger.info(f"WAN {wan_idx} 请求成功, 服务器看到的IP: {result.get('your_ip')}")
                    
                    # 保存结果
                    result_data = {
                        "wan_idx": wan_idx,
                        "success": True,
                        "elapsed": elapsed,
                        "status_code": 200,
                        "server_response": result,
                        "timestamp": time.time()
                    }
                    self.results.append(result_data)
                    return result_data
                else:
                    raise Exception("无法解析JSON响应")
                    
            finally:
                sock.close()
                
        except Exception as e:
            logger.error(f"WAN {wan_idx} 请求异常: {str(e)}")
            result_data = {
                "wan_idx": wan_idx,
                "success": False,
                "error": str(e),
                "timestamp": time.time()
            }
            self.results.append(result_data)
            return result_data
    
    def test_all_wans(self):
        """测试所有可用的WAN接口"""
        logger.info("开始测试所有WAN接口...")
        
        for wan_idx in self.available_wans:
            self.request_via_wan(wan_idx)
            time.sleep(1)  # 防止请求太快
        
        # 打印结果摘要
        success_count = sum(1 for r in self.results if r.get("success", False))
        logger.info(f"WAN测试完成: {success_count}/{len(self.results)} 成功")
        
        unique_ips = set()
        for result in self.results:
            if result.get("success"):
                ip = result.get("server_response", {}).get("your_ip")
                if ip:
                    unique_ips.add(ip)
        
        logger.info(f"服务器看到的不同IP数量: {len(unique_ips)}")
        if unique_ips:
            logger.info(f"具体IP: {', '.join(unique_ips)}")
        
        # 查看请求历史
        try:
            history_response = requests.get(SERVER_URL.replace('/test', '/history'))
            if history_response.status_code == 200:
                history_data = history_response.json()
                logger.info(f"服务器记录的总请求数: {history_data.get('count', 0)}")
        except Exception as e:
            logger.error(f"获取服务器历史记录失败: {str(e)}")
    
    def save_results(self, filename="wan_test_results.json"):
        """保存测试结果到文件"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"测试结果已保存到 {filename}")
    
    def cleanup(self):
        """清理资源"""
        wan_monitor.stop_monitoring()
        logger.info("WAN测试完成,资源已清理")


def main():
    """主函数"""
    logger.info("WAN测试客户端启动")
    
    tester = WANTester()
    try:
        tester.test_all_wans()
        tester.save_results()
    finally:
        tester.cleanup()


if __name__ == "__main__":
    main() 