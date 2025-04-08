#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
demo_gorepe.py - gorepe.py 演示脚本

该脚本演示了如何使用 gorepe.py 进行任务调度和执行，
包括了如何启动单个或多个数据获取任务的示例。
"""
import os
import sys
import time
import logging
import argparse
from pathlib import Path

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('demo_gorepe')

def run_gorepe_with_options(options=None):
    """
    使用指定选项运行 gorepe.py
    
    Args:
        options: 命令行选项列表
    """
    if options is None:
        options = []
        
    # 构建命令行
    cmd = [sys.executable, 'gorepe.py'] + options
    
    # 打印将要执行的命令
    logger.info(f"执行命令: {' '.join(cmd)}")
    
    # 执行命令
    return os.system(' '.join(cmd))


def demo_run_all():
    """执行所有任务"""
    logger.info("示例1: 运行所有任务")
    return run_gorepe_with_options()


def demo_run_init_only():
    """只运行初始化任务"""
    logger.info("示例2: 只运行初始化任务")
    return run_gorepe_with_options(['--only-init'])


def demo_run_parallel_only():
    """只运行并行任务"""
    logger.info("示例3: 只运行并行任务")
    return run_gorepe_with_options(['--only-parallel'])


def demo_run_with_custom_workers():
    """使用自定义工作进程数"""
    logger.info("示例4: 使用自定义工作进程数")
    return run_gorepe_with_options(['--max-workers', '4'])


def demo_run_single_task(task_path):
    """
    运行单个任务
    
    Args:
        task_path: 任务脚本路径
    """
    logger.info(f"示例5: 运行单个任务: {task_path}")
    
    # 检查文件是否存在
    if not os.path.exists(task_path):
        logger.error(f"任务文件不存在: {task_path}")
        return 1
        
    # 创建临时配置文件
    temp_config = "temp_task_config.yaml"
    with open(temp_config, "w") as f:
        f.write(f"parallel_programs:\n  - {task_path}\n")
        
    try:
        # 使用临时配置运行 gorepe.py
        return run_gorepe_with_options(['--config', temp_config])
    finally:
        # 删除临时配置文件
        if os.path.exists(temp_config):
            os.remove(temp_config)


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="gorepe.py 演示脚本")
    
    # 示例类型选项
    parser.add_argument("--demo", type=int, choices=range(1, 6),
                        help="运行指定的示例 (1-5)")
    parser.add_argument("--single-task", type=str,
                        help="运行单个指定的任务文件")
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()
    
    # 如果没有参数，显示帮助
    if len(sys.argv) == 1:
        logger.info("gorepe.py 演示脚本")
        logger.info("可用示例:")
        logger.info("  1. 运行所有任务:       python demo_gorepe.py --demo 1")
        logger.info("  2. 只运行初始化任务:   python demo_gorepe.py --demo 2")
        logger.info("  3. 只运行并行任务:     python demo_gorepe.py --demo 3")
        logger.info("  4. 使用自定义工作进程: python demo_gorepe.py --demo 4")
        logger.info("  5. 运行单个任务:       python demo_gorepe.py --demo 5")
        logger.info("  或者直接:              python demo_gorepe.py --single-task daily_fetcher.py")
        return 0
    
    # 运行指定示例
    if args.demo:
        if args.demo == 1:
            return demo_run_all()
        elif args.demo == 2:
            return demo_run_init_only()
        elif args.demo == 3:
            return demo_run_parallel_only()
        elif args.demo == 4:
            return demo_run_with_custom_workers()
        elif args.demo == 5:
            # 如果没有指定单个任务，使用一个默认任务
            task = args.single_task or "daily_fetcher.py"
            return demo_run_single_task(task)
    
    # 直接运行单个任务
    if args.single_task:
        return demo_run_single_task(args.single_task)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())