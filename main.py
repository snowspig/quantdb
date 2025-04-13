#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QuantDB主程序入口

该脚本用于启动QuantDB并初始化各个模块。
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime

# -- 提前进行参数解析和日志配置 --

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="QuantDB主程序")
    parser.add_argument("--debug", action="store_true", help="是否开启调试模式")
    parser.add_argument("--config", type=str, default="config.yml", help="配置文件路径")
    parser.add_argument("--mode", type=str, default="server", choices=["server", "client"], help="运行模式")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="服务器地址")
    parser.add_argument("--port", type=int, default=5000, help="服务器端口")
    # 直接在这里解析，但注意避免在非主脚本执行时运行
    # 后续可以考虑更复杂的结构，但目前为了调试，直接解析
    if __name__ == "__main__":
        # 只有在作为主脚本运行时才解析参数
        # 在测试或导入时，可以给 args 一个默认值或不同的处理方式
        args = parser.parse_args()
    else:
        # 如果被导入，创建一个包含默认值的 Namespace 对象
        args = argparse.Namespace(debug=False, config='config.yml', mode='server', host='127.0.0.1', port=5000)
    # 需要返回解析结果
    return args

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logger = logging.getLogger("QuantDB") # 获取根 logger 或 QuantDB logger

# -- 在顶层调用 parse_args 获取 args 对象 --
args = parse_args()

# 根据提前解析的参数配置初始日志级别
# 使用 force=True 避免潜在的冲突，并确保级别能设置
initial_log_level = logging.DEBUG if args.debug else logging.INFO
logging.basicConfig(level=initial_log_level, format=LOG_FORMAT, force=True)
logger.info(f"Initial log level set to: {logging.getLevelName(initial_log_level)} based on args.debug={args.debug}")

# -- 添加项目根目录到 sys.path --
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# -- 显式创建 ConfigManager 实例 --
from core.config_manager import ConfigManager # 导入类
logger.debug("Creating ConfigManager instance...")
config_manager = ConfigManager() # 首次创建实例，会执行初始化
logger.debug("ConfigManager instance created.")

# -- 现在可以安全导入其他 core 模块了 --
logger.debug("Importing other core modules...")
# from core import config_manager # 不再需要，我们已经创建了实例
from core import BaseFetcher
import core.utils as utils
# 导入WAN管理相关组件的getter函数
from core.wan_manager import get_port_allocator, get_load_balancer, get_coordinator
from core.network_manager import get_network_manager # 从模块导入 getter
logger.debug("Core modules imported.")

def init_system(debug_mode: bool = False):
    """初始化系统"""
    logger.info("Initializing system...")
    # 加载配置 - 直接使用全局的 config_manager 实例
    # 注意：config_manager 实例已在顶层创建
    config = config_manager.get_all_config()
    
    # 初始化日志 - 再次配置以应用配置文件中的设置
    config_debug = config.get("debug", False)
    # 优先级：命令行 > 配置文件
    final_log_level = logging.DEBUG if debug_mode or config_debug else logging.INFO
    # 再次使用 force=True
    if logging.getLogger().getEffectiveLevel() != final_log_level:
        logging.basicConfig(level=final_log_level, format=LOG_FORMAT, force=True)
        logger.info(f"Log level reconfigured by init_system to: {logging.getLevelName(final_log_level)}")
    else:
        logger.debug(f"Log level already set to {logging.getLevelName(final_log_level)}, no reconfiguration needed in init_system.")
    
    # 初始化数据目录
    data_dir = config.get("data_dir", "data")
    utils.ensure_dir(data_dir)
    
    # 初始化缓存目录
    cache_dir = config.get("cache_dir", "cache")
    utils.ensure_dir(cache_dir)
    
    # 初始化日志目录
    log_dir = config.get("log_dir", "logs")
    utils.ensure_dir(log_dir)
    
    # 初始化WAN管理
    try:
        # 获取端口分配器 (重命名变量避免冲突)
        port_allocator_instance = get_port_allocator()
        if port_allocator_instance:
            logger.info(f"获取端口分配器成功，尝试获取可用WAN接口...")
            wan_indices = port_allocator_instance.get_available_wan_indices()
            logger.info(f"可用WAN接口: {wan_indices}")
        else:
            logger.warning("获取端口分配器失败，返回值为 None")

        # 获取负载均衡器
        load_balancer_instance = get_load_balancer()
        logger.info(f"获取负载均衡器: {load_balancer_instance}")

        # 获取WAN协调器
        coordinator_instance = get_coordinator()
        logger.info(f"获取WAN协调器: {coordinator_instance}")

        # 获取并使用网络管理器实例
        nm_instance = get_network_manager()
        if nm_instance:
            logger.info(f"获取网络管理器实例成功: {nm_instance}")
            # 检查 WAN 是否启用，再访问 active_interfaces
            if nm_instance.wan_enabled:
                logger.info("多WAN口已启用，尝试获取活动接口...")
                active_interfaces_list = nm_instance.active_interfaces
                logger.info(f"网络管理器活动接口数量: {len(active_interfaces_list)}")
            else:
                logger.info("多WAN口未启用，跳过活动接口检查。")
        else:
            logger.warning("获取网络管理器实例失败 (get_network_manager() 返回 None)")

    except Exception as e:
        # 添加 exc_info=True 打印堆栈信息
        logger.error(f"初始化WAN管理组件时发生异常: {str(e)}", exc_info=True)
    
    logger.info("系统初始化完成")

def main():
    """主函数"""
    # 参数解析和初始日志配置已移到顶层
    logger.info(f"QuantDB main function started. Running in {args.mode} mode.")

    # 初始化系统，传递命令行调试标志
    # 注意：init_system 内部会再次检查并可能重新配置日志级别
    init_system(debug_mode=args.debug)
    
    # 根据运行模式启动服务
    if args.mode == "server":
        logger.info("以服务器模式启动QuantDB")
        # 启动API服务器代码...
    else:
        logger.info("以客户端模式启动QuantDB")
        # 启动命令行客户端代码...
    
    logger.info("QuantDB已启动")

if __name__ == "__main__":
    # 参数解析已在顶层完成，日志也已初步配置
    main()