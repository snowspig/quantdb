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

# 修复导入配置管理器
from core.config_manager import ConfigManager
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
SERIAL_WORKERS = 1  # 串行任务始终使用1个线程
# 确保并行线程数至少为1，即使单核CPU
PARALLEL_WORKERS = max(1, TOTAL_WORKERS - SERIAL_WORKERS)  # 并行任务工作线程数

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
        self.verbose = False  # 默认不使用详细日志
        
        # 使用配置管理器来检查配置是否已经验证过
        self.config_state_manager = ConfigManager()
        # 获取验证状态
        validation_status = self.config_state_manager.get_validation_status()
        # 检查是否所有配置都已验证
        self.configurations_validated = all(validation_status.values())
        
        # 共享配置文件路径
        self.shared_config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                              "temp", "shared_config.json")
        # 确保temp目录存在
        os.makedirs(os.path.dirname(self.shared_config_path), exist_ok=True)
        
        # 加载配置文件
        self.load_config()
        
        # 创建共享配置
        self.create_shared_config()
        
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
            resolved_path = self._resolve_task_path(task)
            if not os.path.exists(resolved_path):
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
            resolved_program = self._resolve_task_path(program)
            logger.info(f"开始执行：{program} (实际路径: {resolved_program})")
            start_time = time.time()
            
            # 检查文件是否存在
            if not os.path.exists(resolved_program):
                return False, f"文件不存在：{program}"
                
            # 检查是否可执行
            if not os.access(resolved_program, os.X_OK):
                # 给予执行权限
                os.chmod(resolved_program, 0o755)
            
            # 准备环境变量
            env = os.environ.copy()
            # 添加配置状态信息
            validation_status = self.config_state_manager.get_validation_status()
            for key, value in validation_status.items():
                env[f"QUANTDB_CONFIG_{key.upper()}"] = str(value)
            
            # 环境变量支持传递更详细的验证状态
            env["QUANTDB_CONFIG_VALIDATED"] = str(all(validation_status.values()))
            
            # 添加共享配置文件路径到环境变量
            env["QUANTDB_SHARED_CONFIG"] = self.shared_config_path
            
            # 准备命令行参数
            cmd_args = [sys.executable, resolved_program]
            
            # 添加配置参数 - 使用--config而不是--config-file
            cmd_args.extend(["--config", self.config_file])
            cmd_args.extend(["--shared-config", self.shared_config_path])
            
            # 如果有验证状态，添加到命令行
            if all(validation_status.values()):
                cmd_args.append("--skip-validation")
            
            # 默认不使用详细日志模式，减少输出量
            # 如果需要详细日志，可以通过参数传递
            if not self.verbose:
                # 不传递--verbose参数，使子进程使用简洁日志模式
                pass
            else:
                # 传递--verbose参数，使子进程使用详细日志模式
                cmd_args.append("--verbose")
            
            # 启动子进程，同时传递环境变量和命令行参数
            # 添加encoding='utf-8'参数解决GBK编码问题
            process = subprocess.Popen(
                cmd_args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                env=env,
                encoding='utf-8'
            )
            
            # 等待进程完成并获取输出
            stdout, stderr = process.communicate(timeout=timeout)
            
            # 检查返回码
            if process.returncode == 0:
                # 正常退出 - 成功
                end_time = time.time()
                duration = end_time - start_time
                logger.info(f"程序执行成功 [{program}]，耗时：{duration:.2f} 秒")
                return True, stdout
            elif process.returncode == 2:
                # 退出码为2 - 表示正常执行但没有数据
                end_time = time.time()
                duration = end_time - start_time
                logger.info(f"程序执行成功 [{program}]，但没有数据，耗时：{duration:.2f} 秒")
                # 当退出码为2时，仍然视为成功，但记录详细信息
                return True, f"无数据返回: {stderr}"
            else:
                # 其他非零退出码 - 失败
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
        validation_updates = {}
        
        total_programs = len(self.init_programs)
        completed_programs = 0
        success_count = 0
        
        start_time = time.time()
        
        for program in self.init_programs:
            if not RUNNING:
                logger.warning("收到中断信号，停止执行初始化程序")
                return False
            
            completed_programs += 1
            program_start = time.time()
            success, output = self.run_program(program)
            program_duration = time.time() - program_start
            
            with self.result_lock:
                if success:
                    self.successful_tasks.append(program)
                    success_count += 1
                    # 如果是验证配置程序，更新状态
                    if "validate_configurations.py" in program:
                        logger.info("配置验证成功，更新配置验证状态")
                        # 更新配置状态
                        self.configurations_validated = True
                        # 刷新配置状态管理器中的验证状态
                        self.config_state_manager.update_validation_state(all_valid=True)
                else:
                    self.failed_tasks.append(program)
                    all_success = False
            
            # 计算进度和剩余时间        
            progress = completed_programs / total_programs * 100
            elapsed = time.time() - start_time
            avg_time_per_program = elapsed / completed_programs
            remaining = avg_time_per_program * (total_programs - completed_programs)
            
            # 显示进度
            logger.info(f"初始化进度：{completed_programs}/{total_programs} ({progress:.1f}%)，"
                       f"程序：{program}，结果：{'成功' if success else '失败'}，"
                       f"本次耗时：{program_duration:.1f}秒，"
                       f"已用时间：{elapsed:.1f}秒，预计剩余：{remaining:.1f}秒")
        
        total_duration = time.time() - start_time
        logger.info(f"初始化程序执行完成，成功：{success_count}/{total_programs}，"
                   f"总耗时：{total_duration:.1f}秒")
        
        return all_success
    
    def run_serial_programs(self) -> bool:
        """
        按顺序运行串行程序
        
        每个串行程序将按顺序执行，但与并行程序队列并发运行
        
        Returns:
            bool: 是否全部成功
        """
        if not self.serial_programs:
            logger.info("没有需要执行的串行程序")
            return True
        
        logger.info(f"开始执行 {len(self.serial_programs)} 个串行程序（使用1个专用线程）")
        
        all_success = True
        total_programs = len(self.serial_programs)
        completed_programs = 0
        success_count = 0
        
        start_time = time.time()
        
        for program in self.serial_programs:
            if not RUNNING:
                logger.warning("收到中断信号，停止执行串行程序")
                return False
            
            completed_programs += 1
            program_start = time.time()
            success, output = self.run_program(program)
            program_duration = time.time() - program_start
            
            with self.result_lock:
                if success:
                    self.successful_tasks.append(program)
                    success_count += 1
                else:
                    self.failed_tasks.append(program)
                    all_success = False
            
            # 计算进度和剩余时间        
            progress = completed_programs / total_programs * 100
            elapsed = time.time() - start_time
            avg_time_per_program = elapsed / completed_programs
            remaining = avg_time_per_program * (total_programs - completed_programs)
            
            # 显示进度
            logger.info(f"串行进度：{completed_programs}/{total_programs} ({progress:.1f}%)，"
                       f"程序：{program}，结果：{'成功' if success else '失败'}，"
                       f"本次耗时：{program_duration:.1f}秒，"
                       f"已用时间：{elapsed:.1f}秒，预计剩余：{remaining:.1f}秒")
        
        total_duration = time.time() - start_time
        logger.info(f"串行程序执行完成，成功：{success_count}/{total_programs}，"
                   f"总耗时：{total_duration:.1f}秒")
        
        return all_success
    
    def run_parallel_programs(self, max_workers: int = None) -> bool:
        """
        并行运行程序
        
        在多个线程中并行执行程序，与串行程序队列并发运行
        
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
        
        logger.info(f"开始并行执行 {len(self.parallel_programs)} 个程序（使用{max_workers}个并行线程）")
        
        # 进度追踪变量
        total_programs = len(self.parallel_programs)
        completed_programs = 0
        success_count = 0
        
        # 使用线程池并行执行
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_program = {
                executor.submit(self.run_program, program): program 
                for program in self.parallel_programs
            }
            
            start_time = time.time()
            
            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_program):
                program = future_to_program[future]
                completed_programs += 1
                
                if not RUNNING:
                    logger.warning("收到中断信号，等待已提交任务完成")
                    break
                
                try:
                    success, output = future.result()
                    
                    with self.result_lock:
                        if success:
                            self.successful_tasks.append(program)
                            success_count += 1
                        else:
                            self.failed_tasks.append(program)
                            
                    # 计算进度和剩余时间
                    progress = completed_programs / total_programs * 100
                    elapsed = time.time() - start_time
                    remaining = (elapsed / completed_programs) * (total_programs - completed_programs) if completed_programs > 0 else 0
                    
                    # 显示进度
                    logger.info(f"并行进度：{completed_programs}/{total_programs} ({progress:.1f}%)，"
                               f"程序：{program}，结果：{'成功' if success else '失败'}，"
                               f"已用时间：{elapsed:.1f}秒，预计剩余：{remaining:.1f}秒")
                    
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
        force_validation = options.get('force_validation', False)
        
        # 检查配置状态，如果需要强制验证或之前未验证过，将不跳过初始化
        if force_validation or not self.configurations_validated:
            logger.info("配置尚未验证或需要强制验证，将执行初始化程序")
            skip_init = False
        else:
            logger.info("配置已验证，可以根据选项跳过初始化程序")
        
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
        
        # 1. 运行初始化程序（如果需要）
        # 初始化程序仍需要先完成，因为这可能包含验证配置等重要前置任务
        if not skip_init and not only_serial and not only_parallel:
            init_success = self.run_init_programs()
            
        # 如果只运行初始化程序，则直接返回
        if only_init:
            self.end_time = datetime.now()
            return self.generate_report()
            
        # 2. 同时运行串行和并行程序(如果不是仅执行初始化)
        # 使用线程同时启动串行和并行程序队列
        if not only_init:
            # 设置线程结果
            serial_result = True  # 默认成功
            parallel_result = True # 默认成功
            serial_thread = None
            parallel_thread = None
            
            # 定义串行执行线程函数
            def run_serial():
                nonlocal serial_result
                if not only_parallel:
                    logger.info("启动串行程序队列执行")
                    serial_result = self.run_serial_programs()
                return serial_result
                
            # 定义并行执行线程函数
            def run_parallel():
                nonlocal parallel_result
                if not only_serial: 
                    logger.info("启动并行程序队列执行")
                    parallel_result = self.run_parallel_programs(max_workers)
                return parallel_result
            
            # 同时启动两个线程
            if not only_parallel:
                serial_thread = threading.Thread(target=run_serial)
                serial_thread.start()
            
            if not only_serial:
                parallel_thread = threading.Thread(target=run_parallel)
                parallel_thread.start()
            
            # 等待线程完成
            if serial_thread:
                serial_thread.join()
                serial_success = serial_result
                
            if parallel_thread:
                parallel_thread.join()
                parallel_success = parallel_result
        
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

    def _resolve_task_path(self, task_path: str) -> str:
        """
        解析任务路径，支持绝对路径和相对路径
        如果是相对路径且文件不存在，尝试从data_fetcher/interfaces/目录下查找
        
        Args:
            task_path: 配置中的任务路径
            
        Returns:
            str: 解析后的实际路径
        """
        # 如果是绝对路径或已存在，直接返回
        if os.path.isabs(task_path) or os.path.exists(task_path):
            return task_path
        
        # 尝试从data_fetcher/interfaces/目录查找
        alternate_path = os.path.join("data_fetcher", "interfaces", task_path)
        if os.path.exists(alternate_path):
            return alternate_path
        
        # 返回原路径，让验证步骤报告不存在
        return task_path

    def create_shared_config(self):
        """
        创建共享配置文件，包含所有必要的设置供子进程使用
        """
        try:
            # 获取验证状态
            validation_status = self.config_state_manager.get_validation_status()
            
            # 创建共享配置字典
            shared_config = {
                "config_file": self.config_file,
                "validation_status": validation_status,
                "timestamp": time.time(),
                # 可以添加其他需要共享的配置
            }
            
            # 写入文件
            with open(self.shared_config_path, 'w', encoding='utf-8') as f:
                import json
                json.dump(shared_config, f, indent=2)
                
            logger.info(f"共享配置已创建：{self.shared_config_path}")
            
        except Exception as e:
            logger.error(f"创建共享配置失败：{str(e)}")


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
    parser.add_argument("--force-config-check", action="store_true",
                        help="强制重新验证所有配置（MongoDB、Tushare、WAN等）")
                        
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
        # 创建配置管理器
        config_manager = ConfigManager()
        
        # 检查配置状态，仅在需要时验证配置
        validation_status = config_manager.get_validation_status()
        logging.info(f"配置验证状态: MongoDB={validation_status['mongo']}, "
                     f"Tushare={validation_status['tushare']}, WAN={validation_status['wan']}")
                     
        # 如果指定强制检查配置或配置尚未验证，则执行配置验证
        if args.force_config_check or not all(validation_status.values()):
            logging.info("开始验证系统配置...")
            # 使用ConfigManager的verify_and_store_config方法
            config_state = config_manager.verify_and_store_config(force_check=True)
            # 重新获取验证状态用于记录
            validation_status = config_manager.get_validation_status()
            if not all(validation_status.values()):
                logging.error("配置验证失败，部分组件可能无法正常工作")
        else:
            logging.info("使用已缓存的配置验证结果")
            
        # 创建任务管理器
        manager = TaskManager(config_file=args.config)
        # 设置配置管理器
        manager.config_state_manager = config_manager
        # 设置详细日志模式
        manager.verbose = args.verbose
        
        # 调整并行工作线程数
        actual_parallel_workers = args.max_workers
        if actual_parallel_workers >= TOTAL_WORKERS:
            # 如果请求的并行线程数大于等于总线程数，保留一个给串行队列
            actual_parallel_workers = max(1, TOTAL_WORKERS - SERIAL_WORKERS)
            logger.info(f"调整并行工作线程数为 {actual_parallel_workers}，为串行队列保留 {SERIAL_WORKERS} 个线程")
        
        # 运行选项
        options = {
            'only_init': args.only_init,
            'skip_init': args.skip_init,
            'only_serial': args.only_serial,
            'only_parallel': args.only_parallel,
            'max_workers': actual_parallel_workers,
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