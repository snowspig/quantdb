#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
gorepe.py - 任务调度和并行执行工具

该脚本用于调度和执行多个数据获取任务，支持初始化、串行和并行执行模式，
可以根据配置文件自动调度多个数据获取脚本，实现批量数据更新。

使用方法:
    python gorepe.py  # 使用默认配置运行所有任务
    python gorepe.py --config custom_config.yaml  # 指定自定义配置文件
    python gorepe.py --only-init  # 仅执行初始化程序
    python gorepe.py --skip-init  # 跳过初始化程序
    python gorepe.py --only-serial  # 仅执行串行程序
    python gorepe.py --only-parallel  # 仅执行并行程序
    python gorepe.py --max-workers 8  # 指定并行工作进程数
"""
import os
import sys
import time
import yaml
import signal
import logging
import argparse
import threading
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional, Any, Union
import multiprocessing
import concurrent.futures
import queue

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/gorepe.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('gorepe')

# 全局变量
DEFAULT_CONFIG_FILE = 'tasks_config.yaml'
DEFAULT_TIMEOUT = 7200  # 默认子进程超时时间（秒）
TOTAL_WORKERS = multiprocessing.cpu_count()  # 总工作进程数（根据CPU核心数）
SERIAL_WORKERS = 1  # 串行任务工作进程数
PARALLEL_WORKERS = max(1, TOTAL_WORKERS - 1)  # 并行任务工作进程数

# 运行状态
RUNNING = True

class TaskManager:
    """任务管理器类，负责加载、调度和执行任务"""
    
    def __init__(self, config_file: str = DEFAULT_CONFIG_FILE):
        """
        初始化任务管理器
        
        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file
        self.config = {}
        self.init_programs = []
        self.serial_programs = []
        self.parallel_programs = []
        self.successful_tasks = []
        self.failed_tasks = []
        self.skipped_tasks = []
        self.result_lock = threading.RLock()
        self.start_time = None
        self.end_time = None
        
        # 加载配置文件
        self.load_config()
        
    def load_config(self) -> bool:
        """
        加载任务配置文件
        
        Returns:
            bool: 是否加载成功
        """
        try:
            logger.info(f"加载配置文件：{self.config_file}")
            
            if not os.path.exists(self.config_file):
                logger.error(f"配置文件不存在：{self.config_file}")
                return False
                
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
                
            # 解析配置
            self.init_programs = self.config.get('init_programs', [])
            self.serial_programs = self.config.get('serial_programs', [])
            self.parallel_programs = self.config.get('parallel_programs', [])
            
            logger.info(f"配置加载成功，初始化程序：{len(self.init_programs)}，"
                       f"串行程序：{len(self.serial_programs)}，"
                       f"并行程序：{len(self.parallel_programs)}")
            return True
            
        except Exception as e:
            logger.error(f"加载配置文件失败：{str(e)}")
            return False
    
    def validate_tasks(self) -> List[str]:
        """
        验证所有任务是否存在
        
        Returns:
            List[str]: 不存在的任务列表
        """
        missing_tasks = []
        all_tasks = self.init_programs + self.serial_programs + self.parallel_programs
        
        for task in all_tasks:
            if not os.path.exists(task):
                missing_tasks.append(task)
                
        return missing_tasks
    
    def run_program(self, program: str, timeout: int = DEFAULT_TIMEOUT) -> Tuple[bool, str]:
        """
        运行单个程序
        
        Args:
            program: 程序路径
            timeout: 超时时间（秒）
            
        Returns:
            Tuple[bool, str]: (是否成功, 输出/错误信息)
        """
        if not RUNNING:
            return False, "已中断执行"
            
        try:
            logger.info(f"开始执行：{program}")
            start_time = time.time()
            
            # 检查文件是否存在
            if not os.path.exists(program):
                return False, f"文件不存在：{program}"
                
            # 检查是否可执行
            if not os.access(program, os.X_OK):
                # 给予执行权限
                os.chmod(program, 0o755)
            
            # 启动子进程
            process = subprocess.Popen(
                [sys.executable, program],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # 等待进程完成并获取输出
            stdout, stderr = process.communicate(timeout=timeout)
            
            # 检查返回码
            if process.returncode != 0:
                logger.error(f"程序执行失败 [{program}]，退出代码：{process.returncode}")
                logger.error(f"错误信息：{stderr}")
                return False, stderr
                
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"程序执行成功 [{program}]，耗时：{duration:.2f} 秒")
            
            return True, stdout
            
        except subprocess.TimeoutExpired:
            logger.error(f"程序执行超时 [{program}]，超时时间：{timeout} 秒")
            return False, f"执行超时，超过 {timeout} 秒"
            
        except Exception as e:
            logger.error(f"程序执行异常 [{program}]：{str(e)}")
            return False, str(e)
    
    def run_init_programs(self) -> bool:
        """
        按顺序运行初始化程序
        
        Returns:
            bool: 是否全部成功
        """
        if not self.init_programs:
            logger.info("没有需要执行的初始化程序")
            return True
            
        logger.info(f"开始执行 {len(self.init_programs)} 个初始化程序")
        all_success = True
        
        for program in self.init_programs:
            if not RUNNING:
                logger.warning("收到中断信号，停止执行初始化程序")
                return False
                
            success, output = self.run_program(program)
            
            with self.result_lock:
                if success:
                    self.successful_tasks.append(program)
                else:
                    self.failed_tasks.append(program)
                    all_success = False
                    
        logger.info(f"初始化程序执行完成，成功：{len([p for p in self.init_programs if p in self.successful_tasks])}，"
                   f"失败：{len([p for p in self.init_programs if p in self.failed_tasks])}")
        return all_success
    
    def run_serial_programs(self) -> bool:
        """
        按顺序运行串行程序
        
        Returns:
            bool: 是否全部成功
        """
        if not self.serial_programs:
            logger.info("没有需要执行的串行程序")
            return True
            
        logger.info(f"开始执行 {len(self.serial_programs)} 个串行程序")
        all_success = True
        
        for program in self.serial_programs:
            if not RUNNING:
                logger.warning("收到中断信号，停止执行串行程序")
                return False
                
            success, output = self.run_program(program)
            
            with self.result_lock:
                if success:
                    self.successful_tasks.append(program)
                else:
                    self.failed_tasks.append(program)
                    all_success = False
                    
        logger.info(f"串行程序执行完成，成功：{len([p for p in self.serial_programs if p in self.successful_tasks])}，"
                   f"失败：{len([p for p in self.serial_programs if p in self.failed_tasks])}")
        return all_success
    
    def run_parallel_programs(self, max_workers: int = None) -> bool:
        """
        并行运行程序
        
        Args:
            max_workers: 最大工作进程数
            
        Returns:
            bool: 是否全部成功
        """
        if not self.parallel_programs:
            logger.info("没有需要执行的并行程序")
            return True
            
        # 如果未指定，使用默认的并行工作进程数
        if max_workers is None:
            max_workers = PARALLEL_WORKERS
            
        logger.info(f"开始并行执行 {len(self.parallel_programs)} 个程序，最大工作进程数：{max_workers}")
        
        # 使用线程池并行执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_program = {
                executor.submit(self.run_program, program): program 
                for program in self.parallel_programs
            }
            
            # 处理完成的任务
            completed = 0
            total = len(future_to_program)
            
            for future in concurrent.futures.as_completed(future_to_program):
                program = future_to_program[future]
                completed += 1
                
                if not RUNNING:
                    logger.warning("收到中断信号，等待已提交任务完成")
                    break
                
                try:
                    success, output = future.result()
                    
                    with self.result_lock:
                        if success:
                            self.successful_tasks.append(program)
                        else:
                            self.failed_tasks.append(program)
                            
                    # 显示进度
                    logger.info(f"进度：{completed}/{total} ({completed/total*100:.1f}%)，"
                               f"程序：{program}，结果：{'成功' if success else '失败'}")
                    
                except Exception as e:
                    with self.result_lock:
                        self.failed_tasks.append(program)
                    logger.error(f"执行异常 [{program}]：{str(e)}")
        
        success_count = len([p for p in self.parallel_programs if p in self.successful_tasks])
        fail_count = len([p for p in self.parallel_programs if p in self.failed_tasks])
        logger.info(f"并行程序执行完成，成功：{success_count}，失败：{fail_count}")
        
        return fail_count == 0
    
    def run_all(self, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        运行所有任务
        
        Args:
            options: 运行选项
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        if options is None:
            options = {}
            
        # 解析选项
        only_init = options.get('only_init', False)
        skip_init = options.get('skip_init', False)
        only_serial = options.get('only_serial', False)
        only_parallel = options.get('only_parallel', False)
        max_workers = options.get('max_workers', PARALLEL_WORKERS)
        
        # 记录开始时间
        self.start_time = datetime.now()
        logger.info(f"任务执行开始，时间：{self.start_time}")
        
        # 验证任务文件是否存在
        missing_tasks = self.validate_tasks()
        if missing_tasks:
            logger.error(f"以下任务文件不存在：{', '.join(missing_tasks)}")
            for task in missing_tasks:
                with self.result_lock:
                    self.skipped_tasks.append(task)
                    
        # 根据选项运行任务
        init_success = True
        serial_success = True
        parallel_success = True
        
        # 运行初始化程序
        if not skip_init and not only_serial and not only_parallel:
            init_success = self.run_init_programs()
            
        # 如果只运行初始化程序，则直接返回
        if only_init:
            self.end_time = datetime.now()
            return self.generate_report()
            
        # 运行串行程序
        if not only_parallel and not only_init:
            serial_success = self.run_serial_programs()
            
        # 如果只运行串行程序，则直接返回
        if only_serial:
            self.end_time = datetime.now()
            return self.generate_report()
            
        # 运行并行程序
        if not only_serial and not only_init:
            parallel_success = self.run_parallel_programs(max_workers)
            
        # 记录结束时间
        self.end_time = datetime.now()
        
        # 生成并返回执行报告
        return self.generate_report()
    
    def generate_report(self) -> Dict[str, Any]:
        """
        生成执行报告
        
        Returns:
            Dict[str, Any]: 执行报告
        """
        with self.result_lock:
            # 计算成功率
            total_tasks = len(set(self.init_programs + self.serial_programs + self.parallel_programs))
            success_count = len(set(self.successful_tasks))
            fail_count = len(set(self.failed_tasks))
            skip_count = len(set(self.skipped_tasks))
            
            if total_tasks > 0:
                success_rate = success_count / total_tasks * 100
            else:
                success_rate = 0
                
            # 计算执行时间
            if self.start_time and self.end_time:
                duration = (self.end_time - self.start_time).total_seconds()
            else:
                duration = 0
                
            # 创建执行报告
            report = {
                'status': 'success' if fail_count == 0 else 'partial_success' if success_count > 0 else 'failure',
                'start_time': self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time else None,
                'end_time': self.end_time.strftime('%Y-%m-%d %H:%M:%S') if self.end_time else None,
                'duration': duration,
                'total_tasks': total_tasks,
                'success_count': success_count,
                'fail_count': fail_count,
                'skip_count': skip_count,
                'success_rate': success_rate,
                'successful_tasks': sorted(self.successful_tasks),
                'failed_tasks': sorted(self.failed_tasks),
                'skipped_tasks': sorted(self.skipped_tasks)
            }
            
            # 输出日志
            logger.info("=" * 80)
            logger.info(f"任务执行完成，状态：{report['status']}")
            logger.info(f"开始时间：{report['start_time']}")
            logger.info(f"结束时间：{report['end_time']}")
            logger.info(f"执行时间：{report['duration']:.2f} 秒")
            logger.info(f"总任务数：{report['total_tasks']}")
            logger.info(f"成功任务：{report['success_count']} ({report['success_rate']:.2f}%)")
            logger.info(f"失败任务：{report['fail_count']}")
            logger.info(f"跳过任务：{report['skip_count']}")
            
            if report['failed_tasks']:
                logger.info("失败任务列表：")
                for task in report['failed_tasks']:
                    logger.info(f"  - {task}")
                    
            logger.info("=" * 80)
            
            return report


def signal_handler(sig, frame):
    """处理中断信号"""
    global RUNNING
    logger.warning(f"收到信号 {sig}，准备安全退出...")
    RUNNING = False


def parse_args() -> argparse.Namespace:
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 解析后的参数
    """
    parser = argparse.ArgumentParser(description="gorepe - 任务调度和并行执行工具")
    
    parser.add_argument("--config", type=str, default=DEFAULT_CONFIG_FILE,
                        help=f"配置文件路径 (默认: {DEFAULT_CONFIG_FILE})")
    parser.add_argument("--only-init", action="store_true",
                        help="仅执行初始化程序")
    parser.add_argument("--skip-init", action="store_true",
                        help="跳过初始化程序")
    parser.add_argument("--only-serial", action="store_true",
                        help="仅执行串行程序")
    parser.add_argument("--only-parallel", action="store_true",
                        help="仅执行并行程序")
    parser.add_argument("--max-workers", type=int, default=PARALLEL_WORKERS,
                        help=f"最大工作进程数 (默认: {PARALLEL_WORKERS})")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help=f"任务超时时间(秒) (默认: {DEFAULT_TIMEOUT})")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="显示详细日志")
                        
    return parser.parse_args()


def setup_logger(verbose: bool = False):
    """
    设置日志记录器
    
    Args:
        verbose: 是否显示详细日志
    """
    # 创建日志目录
    log_dir = Path("logs")
    if not log_dir.exists():
        log_dir.mkdir(parents=True)
        
    # 设置日志级别
    level = logging.DEBUG if verbose else logging.INFO
    logging.getLogger('gorepe').setLevel(level)


def main() -> int:
    """
    主函数
    
    Returns:
        int: 退出代码
    """
    # 解析命令行参数
    args = parse_args()
    
    # 设置日志
    setup_logger(args.verbose)
    
    # 设置信号处理
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # 创建任务管理器
        manager = TaskManager(config_file=args.config)
        
        # 运行选项
        options = {
            'only_init': args.only_init,
            'skip_init': args.skip_init,
            'only_serial': args.only_serial,
            'only_parallel': args.only_parallel,
            'max_workers': args.max_workers,
        }
        
        # 运行所有任务
        report = manager.run_all(options)
        
        # 返回执行状态码
        if report['status'] == 'success':
            return 0
        elif report['status'] == 'partial_success':
            return 1
        else:
            return 2
            
    except Exception as e:
        logger.exception(f"执行异常：{str(e)}")
        return 3


if __name__ == "__main__":
    sys.exit(main())