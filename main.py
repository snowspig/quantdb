#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QuantDB 主入口程序
演示如何使用核心模块进行数据获取和处理
"""
import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# 导入核心模块
from core import config_manager, mongodb_handler, network_manager
from core import BaseFetcher
import core.utils as utils
# 导入端口分配器
from core.wan_manager import port_allocator

# 尝试导入xcsc_tushare
try:
    import xcsc_tushare as ts
except ImportError:
    ts = None


def setup_logging():
    """设置日志配置"""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 获取日志配置
    log_config = config_manager.get_log_config()
    log_level = log_config.get('level', 'INFO')
    log_file = log_config.get('file', 'logs/quantdb.log')

    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 获取根日志记录器
    root_logger = logging.getLogger()
    
    # 清除现有的处理器，避免重复
    if root_logger.handlers:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # 设置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)

    # 配置根日志记录器
    root_logger.setLevel(getattr(logging, log_level))
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # 防止日志传递到父记录器，避免重复
    root_logger.propagate = False

    # 同时设置其他常用记录器级别，防止冗余日志
    for logger_name in ['urllib3', 'requests']:
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    return root_logger


def check_system_status():
    """检查系统状态"""
    logger = logging.getLogger("main.check_system_status")
    status = {
        "config": False,
        "database": False,
        "network": False
    }

    # 检查配置
    try:
        config = config_manager.get_all_config()
        status["config"] = bool(config)
        logger.info(f"配置状态: {'正常' if status['config'] else '异常'}")
    except Exception as e:
        logger.error(f"配置检查失败: {str(e)}")

    # 检查数据库
    try:
        if mongodb_handler.is_connected():
            status["database"] = True
            logger.info(f"数据库状态: 正常, 数据库: {mongodb_handler.db.name}")
        else:
            logger.warning("数据库连接失败")
    except Exception as e:
        logger.error(f"数据库检查失败: {str(e)}")

    # 检查网络
    try:
        if network_manager and network_manager.check_connectivity():
            status["network"] = True
            # 获取接口状态
            interfaces_status = network_manager.get_interfaces_status()
            logger.info(f"网络状态: 正常, 多WAN模式: {interfaces_status['enabled']}, 活跃接口: {interfaces_status['active_interfaces']}/{interfaces_status['total_interfaces']}")
        else:
            logger.warning("网络连接异常")
    except Exception as e:
        logger.error(f"网络检查失败: {str(e)}")

    return status


class TushareWANTester:
    """Tushare多WAN测试类"""
    
    def __init__(self):
        """初始化测试器"""
        self.logger = logging.getLogger("main.TushareWANTester")
        
        # 获取Tushare配置
        all_config = config_manager.get_all_config()
        tushare_config = all_config.get('tushare', {})
        self.api_url = tushare_config.get('api_url', 'http://116.128.206.39:7172')
        self.token = tushare_config.get('token', 'b5bb9d57e35cf485f0366eb6581017fb69cefff888da312b0128f3a0')
        
        self.logger.info(f"使用Tushare API URL: {self.api_url}")
        self.logger.info(f"使用Tushare Token: {self.token[:8]}...{self.token[-8:]}")
        
        # 配置xcsc_tushare
        if ts is not None:
            ts.set_token(self.token)
        
        # 获取WAN状态
        self.refresh_wan_status()
        
        # 用于存储最快WAN口信息的配置文件
        self.fastest_wan_config_file = 'wan_fastest.ini'
    
    def refresh_wan_status(self):
        """刷新WAN状态"""
        # 刷新接口状态
        network_manager.refresh_interfaces()
        
        # 获取接口状态
        self.wan_status = network_manager.get_interfaces_status()
        self.active_wans = []
        
        for interface in self.wan_status.get('interfaces', []):
            if interface.get('active') and 'wan_idx' in interface:
                self.active_wans.append(interface.get('wan_idx'))
        
        self.logger.info(f"多WAN状态: {'启用' if self.wan_status['enabled'] else '禁用'}")
        self.logger.info(f"接口总数: {self.wan_status['total_interfaces']}")
        self.logger.info(f"活跃接口数: {self.wan_status['active_interfaces']}")
        self.logger.info(f"活跃WAN索引: {self.active_wans}")
        
        # 打印每个接口的详细信息
        self.logger.info("接口详情:")
        for interface in self.wan_status.get('interfaces', []):
            status = "活跃" if interface.get('active') else "不可用"
            external_ip = interface.get('external_ip', 'N/A')
            self.logger.info(f"  - {interface['name']} ({interface['source_ip']}): {status}, 外部IP: {external_ip}")
        
        return len(self.active_wans) > 0
    
    def test_wan_with_tushare(self, wan_idx):
        """
        通过特定WAN口测试Tushare API
        
        Args:
            wan_idx: WAN索引
            
        Returns:
            Dict: 测试结果
        """
        if ts is None:
            self.logger.error("无法导入xcsc_tushare库，请确保已安装该库")
            return {
                'success': False,
                'error': "xcsc_tushare库不可用",
                'wan_idx': wan_idx
            }
            
        try:
            self.logger.info(f"通过WAN {wan_idx} 测试Tushare API")
            
            # 获取该WAN口的本地端口
            local_port = port_allocator.allocate_port(wan_idx)
            if not local_port:
                self.logger.error(f"无法为WAN {wan_idx} 分配端口")
                return {
                    'success': False,
                    'error': f"无法分配端口",
                    'wan_idx': wan_idx
                }
            
            self.logger.info(f"WAN {wan_idx} 分配端口: {local_port}")
            
            # 创建pro_api实例
            try:
                start_time = time.time()
                
                # 设置环境变量LOCAL_BIND_PORT，供xcsc_tushare内部使用
                os.environ['LOCAL_BIND_PORT'] = str(local_port)
                
                # 创建tushare api对象
                pro = ts.pro_api(self.token, server=self.api_url)
                
                # 获取交易日历数据
                df = pro.trade_cal(exchange='', start_date='20180901', end_date='20181001')
                
                elapsed = time.time() - start_time
                
                # 输出结果
                if df is not None and not df.empty:
                    rows = len(df)
                    self.logger.info(f"WAN {wan_idx} 测试成功，获取到 {rows} 行数据，耗时: {elapsed:.3f}秒")
                    
                    # 显示数据前5行
                    self.logger.info(f"数据预览:\n{df.head()}")
                    
                    # 获取外部IP
                    external_ip = None
                    for interface in self.wan_status.get('interfaces', []):
                        if interface.get('wan_idx') == wan_idx and interface.get('active'):
                            external_ip = interface.get('external_ip')
                            break
                    
                    return {
                        'success': True,
                        'wan_idx': wan_idx,
                        'data_rows': rows,
                        'elapsed': elapsed,
                        'external_ip': external_ip
                    }
                else:
                    self.logger.error(f"WAN {wan_idx} 测试返回空数据")
                    return {
                        'success': False,
                        'error': "返回空数据",
                        'wan_idx': wan_idx
                    }
                    
            except Exception as e:
                self.logger.exception(f"WAN {wan_idx} Tushare API调用异常: {str(e)}")
                return {
                    'success': False,
                    'error': str(e),
                    'wan_idx': wan_idx
                }
            finally:
                # 清除环境变量
                if 'LOCAL_BIND_PORT' in os.environ:
                    del os.environ['LOCAL_BIND_PORT']
                
                # 释放端口
                port_allocator.release_port(wan_idx, local_port)
                
        except Exception as e:
            self.logger.exception(f"WAN {wan_idx} 测试过程发生异常: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'wan_idx': wan_idx
            }
    
    def test_all_wans(self):
        """
        测试所有WAN口（并行执行）
        
        Returns:
            List[Dict]: 测试结果列表
        """
        if not self.active_wans:
            self.logger.warning("没有可用的WAN接口，无法进行测试")
            return []
        
        self.logger.info(f"开始并行测试 {len(self.active_wans)} 个WAN口...")
        
        results = []
        futures = []
        
        # 使用线程池并行执行所有WAN口测试
        with ThreadPoolExecutor(max_workers=len(self.active_wans)) as executor:
            # 提交所有测试任务
            for wan_idx in self.active_wans:
                future = executor.submit(self.test_wan_with_tushare, wan_idx)
                futures.append((future, wan_idx))
            
            # 收集结果
            for future, wan_idx in futures:
                try:
                    result = future.result(timeout=30)  # 30秒超时
                    results.append(result)
                    if result.get('success'):
                        self.logger.info(f"WAN {wan_idx} 并行测试完成: 成功")
                    else:
                        self.logger.error(f"WAN {wan_idx} 并行测试完成: 失败 - {result.get('error', '未知错误')}")
                except Exception as e:
                    self.logger.exception(f"WAN {wan_idx} 测试过程中发生异常: {e}")
                    results.append({
                        'success': False,
                        'error': str(e),
                        'wan_idx': wan_idx
                    })
        
        # 统计结果
        success_count = sum(1 for r in results if r.get('success', False))
        self.logger.info(f"并行WAN测试结果: {success_count}/{len(results)} 成功")
        
        # 计算总体耗时情况
        if success_count > 0:
            avg_time = sum(r.get('elapsed', 0) for r in results if r.get('success', False)) / success_count
            self.logger.info(f"成功请求的平均耗时: {avg_time:.3f}秒")
            
            # 找出最快和最慢的WAN口
            success_results = [r for r in results if r.get('success', False)]
            if success_results:
                fastest = min(success_results, key=lambda x: x.get('elapsed', float('inf')))
                slowest = max(success_results, key=lambda x: x.get('elapsed', 0))
                
                self.logger.info(f"最快的WAN口: WAN {fastest.get('wan_idx')}, 耗时: {fastest.get('elapsed', 0):.3f}秒")
                self.logger.info(f"最慢的WAN口: WAN {slowest.get('wan_idx')}, 耗时: {slowest.get('elapsed', 0):.3f}秒")
                
        # 打印详细结果
        self.logger.info("\n每个WAN口的测试结果详情:")
        for result in results:
            wan_idx = result.get('wan_idx', 'unknown')
            if result.get('success'):
                data_rows = result.get('data_rows', 0)
                req_time = result.get('elapsed', 0)
                external_ip = result.get('external_ip', 'unknown')
                self.logger.info(f"  WAN {wan_idx}: 成功, 数据行数={data_rows}, 耗时={req_time:.3f}秒, 外部IP={external_ip}")
            else:
                error = result.get('error', 'unknown')
                self.logger.error(f"  WAN {wan_idx}: 失败, 错误: {error}")
        
        return results
        
    def test_all_wans_serial(self):
        """
        串行测试所有WAN口
        
        Returns:
            List[Dict]: 测试结果列表
        """
        if not self.active_wans:
            self.logger.warning("没有可用的WAN接口，无法进行测试")
            return []
        
        self.logger.info(f"开始串行测试 {len(self.active_wans)} 个WAN口...")
        results = []
        
        for wan_idx in self.active_wans:
            # 使用Tushare API测试
            self.logger.info(f"串行测试 WAN {wan_idx}...")
            result = self.test_wan_with_tushare(wan_idx)
            results.append(result)
            # 防止请求太频繁
            time.sleep(1)
        
        # 统计串行结果
        serial_success = sum(1 for r in results if r.get('success', False))
        self.logger.info(f"串行WAN测试结果: {serial_success}/{len(results)} 成功")
        
        # 打印详细结果
        self.logger.info("\n每个WAN口的测试结果详情:")
        for result in results:
            wan_idx = result.get('wan_idx', 'unknown')
            if result.get('success'):
                data_rows = result.get('data_rows', 0)
                req_time = result.get('elapsed', 0)
                external_ip = result.get('external_ip', 'unknown')
                self.logger.info(f"  WAN {wan_idx}: 成功, 数据行数={data_rows}, 耗时={req_time:.3f}秒, 外部IP={external_ip}")
            else:
                error = result.get('error', 'unknown')
                self.logger.error(f"  WAN {wan_idx}: 失败, 错误: {error}")
        
        return results
        
    def test_all_wans_parallel_and_serial(self):
        """
        同时执行并行和串行测试，并比较性能差异
        
        Returns:
            Dict: 包含并行和串行测试结果的字典
        """
        self.logger.info("开始并行vs串行对比测试...")
        
        # 并行测试
        self.logger.info("1. 执行并行测试:")
        parallel_start = time.time()
        parallel_results = self.test_all_wans()
        parallel_elapsed = time.time() - parallel_start
        
        # 等待一段时间，确保资源释放
        time.sleep(3)
        
        # 串行测试
        self.logger.info("2. 执行串行测试:")
        serial_start = time.time()
        serial_results = self.test_all_wans_serial()
        serial_elapsed = time.time() - serial_start
        
        # 比较性能
        self.logger.info("\n性能对比结果:")
        parallel_success = sum(1 for r in parallel_results if r.get('success', False))
        serial_success = sum(1 for r in serial_results if r.get('success', False))
        
        self.logger.info(f"- 并行测试总耗时: {parallel_elapsed:.3f}秒, 成功率: {parallel_success}/{len(parallel_results)}")
        self.logger.info(f"- 串行测试总耗时: {serial_elapsed:.3f}秒, 成功率: {serial_success}/{len(serial_results)}")
        
        if parallel_elapsed > 0 and serial_elapsed > 0:
            speedup = serial_elapsed / parallel_elapsed
            self.logger.info(f"- 加速比: {speedup:.2f}x")
        
        return {
            'parallel': {
                'results': parallel_results,
                'elapsed': parallel_elapsed,
                'success_count': parallel_success
            },
            'serial': {
                'results': serial_results,
                'elapsed': serial_elapsed,
                'success_count': serial_success
            }
        }

    def find_fastest_wan(self, test_count=3):
        """
        进行多次测试，找出最快的WAN口并保存结果到配置文件
        
        Args:
            test_count: 测试次数，默认进行3次测试取平均值
            
        Returns:
            Dict: 最快WAN口的信息
        """
        if not self.active_wans:
            self.logger.warning("没有可用的WAN接口，无法测试最快WAN口")
            return None
        
        self.logger.info(f"开始测试最快WAN口 (测试次数: {test_count})...")
        
        # 记录每个WAN口的总耗时
        wan_total_times = {wan_idx: 0.0 for wan_idx in self.active_wans}
        wan_success_counts = {wan_idx: 0 for wan_idx in self.active_wans}
        wan_info = {}
        
        # 进行多次测试
        for i in range(test_count):
            self.logger.info(f"执行第 {i+1}/{test_count} 轮测试...")
            
            # 并行测试所有WAN口
            results = self.test_all_wans()
            
            # 收集每个WAN口的耗时
            for result in results:
                if result.get('success'):
                    wan_idx = result.get('wan_idx')
                    elapsed = result.get('elapsed', 0)
                    external_ip = result.get('external_ip', 'unknown')
                    
                    wan_total_times[wan_idx] += elapsed
                    wan_success_counts[wan_idx] += 1
                    
                    # 保存WAN口信息
                    if wan_idx not in wan_info:
                        # 查找对应的接口信息
                        for interface in self.wan_status.get('interfaces', []):
                            if interface.get('wan_idx') == wan_idx:
                                wan_info[wan_idx] = {
                                    'name': interface.get('name', f'wan{wan_idx}'),
                                    'source_ip': interface.get('source_ip', 'unknown'),
                                    'external_ip': external_ip
                                }
                                break
            
            # 避免频繁测试
            if i < test_count - 1:
                time.sleep(1)
        
        # 计算每个WAN口的平均耗时
        wan_avg_times = {}
        for wan_idx, total_time in wan_total_times.items():
            if wan_success_counts[wan_idx] > 0:
                wan_avg_times[wan_idx] = total_time / wan_success_counts[wan_idx]
                self.logger.info(f"WAN {wan_idx} 平均耗时: {wan_avg_times[wan_idx]:.3f}秒 (成功次数: {wan_success_counts[wan_idx]}/{test_count})")
        
        # 找出平均耗时最短的WAN口
        if wan_avg_times:
            fastest_wan_idx = min(wan_avg_times, key=wan_avg_times.get)
            fastest_wan_time = wan_avg_times[fastest_wan_idx]
            
            fastest_wan = {
                'wan_idx': fastest_wan_idx,
                'average_time': fastest_wan_time,
                'success_rate': f"{wan_success_counts[fastest_wan_idx]}/{test_count}",
                'name': wan_info.get(fastest_wan_idx, {}).get('name', f'wan{fastest_wan_idx}'),
                'source_ip': wan_info.get(fastest_wan_idx, {}).get('source_ip', 'unknown'),
                'external_ip': wan_info.get(fastest_wan_idx, {}).get('external_ip', 'unknown'),
                'test_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.logger.info(f"最快的WAN口是: WAN {fastest_wan_idx} ({fastest_wan['name']}), 平均耗时: {fastest_wan_time:.3f}秒")
            
            # 保存最快WAN口信息到配置文件
            self._save_fastest_wan_config(fastest_wan)
            
            return fastest_wan
        else:
            self.logger.warning("所有WAN口测试均失败，无法确定最快WAN口")
            return None
    
    def _save_fastest_wan_config(self, fastest_wan):
        """
        保存最快WAN口信息到配置文件
        
        Args:
            fastest_wan: Dict, 最快WAN口信息
        """
        import configparser
        
        config = configparser.ConfigParser()
        config['FastestWAN'] = {
            'wan_idx': str(fastest_wan['wan_idx']),
            'name': fastest_wan['name'],
            'source_ip': fastest_wan['source_ip'],
            'external_ip': fastest_wan['external_ip'],
            'average_time': str(fastest_wan['average_time']),
            'success_rate': fastest_wan['success_rate'],
            'test_date': fastest_wan['test_date']
        }
        
        try:
            with open(self.fastest_wan_config_file, 'w') as f:
                config.write(f)
            self.logger.info(f"最快WAN口信息已保存到 {self.fastest_wan_config_file}")
        except Exception as e:
            self.logger.error(f"保存最快WAN口信息失败: {str(e)}")
    
    def get_fastest_wan_from_config(self):
        """
        从配置文件中读取最快WAN口信息
        
        Returns:
            Dict: 最快WAN口信息，如果配置文件不存在或无效则返回None
        """
        import configparser
        import os
        
        if not os.path.exists(self.fastest_wan_config_file):
            self.logger.info(f"最快WAN口配置文件 {self.fastest_wan_config_file} 不存在")
            return None
        
        try:
            config = configparser.ConfigParser()
            config.read(self.fastest_wan_config_file)
            
            if 'FastestWAN' in config:
                fastest_wan = {
                    'wan_idx': config['FastestWAN'].getint('wan_idx'),
                    'name': config['FastestWAN'].get('name'),
                    'source_ip': config['FastestWAN'].get('source_ip'),
                    'external_ip': config['FastestWAN'].get('external_ip'),
                    'average_time': config['FastestWAN'].getfloat('average_time'),
                    'success_rate': config['FastestWAN'].get('success_rate'),
                    'test_date': config['FastestWAN'].get('test_date')
                }
                
                self.logger.info(f"从配置中读取到最快WAN口: WAN {fastest_wan['wan_idx']} ({fastest_wan['name']}), 测试时间: {fastest_wan['test_date']}")
                return fastest_wan
            else:
                self.logger.warning(f"最快WAN口配置文件 {self.fastest_wan_config_file} 格式无效")
                return None
        except Exception as e:
            self.logger.error(f"读取最快WAN口配置失败: {str(e)}")
            return None

    def request_via_fastest_wan(self, api_name=None, params=None):
        """
        通过最快的WAN口发送Tushare API请求
        
        Args:
            api_name: API名称，例如'trade_cal'
            params: API参数
            
        Returns:
            Dict: 请求结果
        """
        if ts is None:
            self.logger.error("无法导入xcsc_tushare库，请确保已安装该库")
            return {
                'success': False,
                'error': "xcsc_tushare库不可用",
                'data': None
            }
        
        # 获取最快WAN口信息
        fastest_wan = self.get_fastest_wan_from_config()
        
        # 如果没有配置最快WAN口记录，或者配置过期，则重新测试
        if fastest_wan is None:
            self.logger.info("没有找到最快WAN口配置，执行测试以确定最快WAN口...")
            fastest_wan = self.find_fastest_wan(test_count=2)
            
            if fastest_wan is None:
                self.logger.error("无法确定最快WAN口，无法执行请求")
                return {
                    'success': False,
                    'error': "无法确定最快WAN口",
                    'data': None
                }
        
        wan_idx = fastest_wan['wan_idx']
        self.logger.info(f"通过最快WAN口 {wan_idx} ({fastest_wan['name']}) 发送API请求")
        
        # 默认API调用参数
        if api_name is None:
            api_name = 'trade_cal'
        
        if params is None:
            params = {
                'exchange': '',
                'start_date': '20180901',
                'end_date': '20181001'
            }
        
        # 获取WAN口的本地端口
        local_port = port_allocator.allocate_port(wan_idx)
        if not local_port:
            self.logger.error(f"无法为WAN {wan_idx} 分配端口")
            return {
                'success': False,
                'error': f"无法分配端口",
                'data': None
            }
        
        try:
            self.logger.info(f"WAN {wan_idx} 分配端口: {local_port}, 准备调用API: {api_name}")
            
            # 设置环境变量LOCAL_BIND_PORT
            os.environ['LOCAL_BIND_PORT'] = str(local_port)
            
            # 记录开始时间
            start_time = time.time()
            
            # 创建tushare api对象
            pro = ts.pro_api(self.token, server=self.api_url)
            
            # 动态调用方法
            api_method = getattr(pro, api_name)
            df = api_method(**params)
            
            # 计算耗时
            elapsed = time.time() - start_time
            
            # 处理结果
            if df is not None and not df.empty:
                rows = len(df)
                self.logger.info(f"API请求成功，获取到 {rows} 行数据，耗时: {elapsed:.3f}秒")
                
                return {
                    'success': True,
                    'data': df,
                    'rows': rows,
                    'elapsed': elapsed,
                    'wan_idx': wan_idx
                }
            else:
                self.logger.error(f"API请求返回空数据")
                return {
                    'success': False,
                    'error': "返回空数据",
                    'data': None,
                    'wan_idx': wan_idx
                }
        except Exception as e:
            self.logger.exception(f"API请求异常: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'data': None,
                'wan_idx': wan_idx
            }
        finally:
            # 清除环境变量
            if 'LOCAL_BIND_PORT' in os.environ:
                del os.environ['LOCAL_BIND_PORT']
            
            # 释放端口
            port_allocator.release_port(wan_idx, local_port)


def test_wan_parallel(parallel=True):
    """
    测试多WAN口并行/串行请求，使用TushareWANTester类
    
    Args:
        parallel: 是否使用并行模式测试
        
    Returns:
        Dict: 测试结果
    """
    logger = logging.getLogger("main.test_wan")
    
    # 检查xcsc_tushare库是否可用
    if ts is None:
        logger.error("无法导入xcsc_tushare库，请确保已安装该库")
        return {'status': 'error', 'message': '无法导入xcsc_tushare库'}
    
    # 创建测试器实例
    tester = TushareWANTester()
    
    # 检查是否有可用的WAN接口
    if not tester.active_wans:
        return {'status': 'error', 'message': '没有可用的WAN接口'}
    
    # 根据参数选择测试模式
    start_time = time.time()
    
    if parallel:
        # 并行测试
        logger.info(f"开始并行测试 {len(tester.active_wans)} 个WAN口...")
        results = tester.test_all_wans()
    else:
        # 串行测试
        logger.info(f"开始串行测试 {len(tester.active_wans)} 个WAN口...")
        results = tester.test_all_wans_serial()
    
    elapsed = time.time() - start_time
    mode_name = "并行" if parallel else "串行"
    
    # 统计成功率
    success_count = sum(1 for r in results if r.get('success', False))
    
    return {
        'status': 'success' if success_count > 0 else 'error',
        'mode': mode_name,
        'results': results,
        'success_count': success_count,
        'total_count': len(results),
        'elapsed': elapsed
    }


def test_wan_comparison():
    """
    执行并行和串行对比测试
    
    Returns:
        Dict: 对比结果
    """
    logger = logging.getLogger("main.test_wan_comparison")
    logger.info("开始执行并行vs串行WAN测试对比...")
    
    # 创建测试器实例
    tester = TushareWANTester()
    
    # 执行对比测试
    return tester.test_all_wans_parallel_and_serial()


class ExampleFetcher(BaseFetcher):
    """
    示例数据获取器
    演示如何实现BaseFetcher的子类
    """
    
    def __init__(self):
        """初始化示例获取器"""
        super().__init__(api_name="example", silent=False)
    
    def fetch_data(self, **kwargs):
        """
        从数据源获取数据
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            pd.DataFrame: 获取的数据
        """
        import pandas as pd
        import numpy as np
        
        # 这里只是一个演示，实际应用中应该从真实数据源获取数据
        self.logger.info(f"正在获取数据，参数: {kwargs}")
        
        # 创建一个示例数据框
        dates = pd.date_range(start=kwargs.get('start_date', '20230101'), 
                             end=kwargs.get('end_date', '20230105'))
        
        # 生成随机股票数据
        n_stocks = kwargs.get('n_stocks', 5)
        stock_codes = [f"60000{i}.SH" for i in range(1, n_stocks+1)]
        
        data = []
        for date in dates:
            date_str = date.strftime('%Y%m%d')
            for ts_code in stock_codes:
                # 生成模拟数据
                open_price = np.random.uniform(10, 100)
                close = open_price * (1 + np.random.uniform(-0.05, 0.05))
                high = max(open_price, close) * (1 + np.random.uniform(0, 0.03))
                low = min(open_price, close) * (1 - np.random.uniform(0, 0.03))
                vol = np.random.uniform(1000, 10000)
                amount = vol * close
                
                data.append({
                    'ts_code': ts_code,
                    'trade_date': date_str,
                    'open': open_price,
                    'high': high,
                    'low': low,
                    'close': close,
                    'vol': vol,
                    'amount': amount,
                    'change': close - open_price,
                    'pct_chg': (close - open_price) / open_price * 100
                })
        
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        # 模拟网络请求延迟
        time.sleep(0.5)
        
        self.logger.info(f"成功获取 {len(df)} 条数据记录")
        return df
    
    def process_data(self, data):
        """
        处理获取的原始数据
        
        Args:
            data: 原始数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        self.logger.info("处理数据...")
        
        # 确保日期列是字符串格式
        if 'trade_date' in data.columns and not data['trade_date'].dtype == 'object':
            data['trade_date'] = data['trade_date'].astype(str)
        
        # 添加计算列
        data['ma5'] = data.groupby('ts_code')['close'].rolling(window=5, min_periods=1).mean().reset_index(drop=True)
        data['ma10'] = data.groupby('ts_code')['close'].rolling(window=10, min_periods=1).mean().reset_index(drop=True)
        
        # 计算涨跌标志
        data['up_down'] = data['change'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        
        self.logger.info(f"数据处理完成，记录数: {len(data)}")
        return data
    
    def get_collection_name(self):
        """
        获取数据存储的集合名称
        
        Returns:
            str: 集合名称
        """
        return "example_daily_data"
    
    def _run_recent_mode(self):
        """
        运行最近数据模式
        
        Returns:
            Dict: 执行结果
        """
        self.logger.info("运行最近数据获取模式")
        
        # 获取今天和昨天的日期
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
        
        # 创建一个获取任务
        task = {
            'start_date': start_date,
            'end_date': end_date,
            'n_stocks': 10
        }
        
        # 执行获取任务
        saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条最近数据"
        }
    
    def _run_date_mode(self, date):
        """
        运行指定日期模式
        
        Args:
            date: 日期字符串
            
        Returns:
            Dict: 执行结果
        """
        self.logger.info(f"运行指定日期数据获取模式: {date}")
        
        # 创建任务
        task = {
            'start_date': date,
            'end_date': date,
            'n_stocks': 10
        }
        
        # 执行任务
        saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条 {date} 日期的数据"
        }
    
    def _run_date_range_mode(self, start_date, end_date):
        """
        运行日期范围模式
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Dict: 执行结果
        """
        self.logger.info(f"运行日期范围数据获取模式: {start_date} 至 {end_date}")
        
        # 创建任务
        task = {
            'start_date': start_date,
            'end_date': end_date,
            'n_stocks': 10
        }
        
        # 执行任务
        saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条从 {start_date} 至 {end_date} 的数据"
        }


def api_request_via_fastest_wan(api_name=None, params=None):
    """
    通过最快的WAN口发送Tushare API请求的便捷函数
    
    Args:
        api_name: API名称，例如'trade_cal'
        params: API参数
        
    Returns:
        Dict: 请求结果
    """
    logger = logging.getLogger("main.api_request_via_fastest_wan")
    
    # 检查xcsc_tushare库是否可用
    if ts is None:
        logger.error("无法导入xcsc_tushare库，请确保已安装该库")
        return {'success': False, 'error': '无法导入xcsc_tushare库', 'data': None}
    
    # 创建TushareWANTester实例
    tester = TushareWANTester()
    
    # 通过最快WAN口发送请求
    return tester.request_via_fastest_wan(api_name, params)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='QuantDB 数据获取和管理系统')
    
    parser.add_argument('--check', action='store_true', help='检查系统状态')
    parser.add_argument('--demo', action='store_true', help='运行示例数据获取器')
    parser.add_argument('--validate-wan', action='store_true', help='验证多WAN配置')
    parser.add_argument('--test-wan', action='store_true', help='测试所有WAN接口')
    parser.add_argument('--find-fastest-wan', action='store_true', help='找出并保存最快的WAN口')
    parser.add_argument('--api-request', action='store_true', help='通过最快的WAN口发送API请求')
    parser.add_argument('--api-name', type=str, default='trade_cal', help='API名称')
    parser.add_argument('--api-params', type=str, help='API参数，JSON格式')
    parser.add_argument('--wan-mode', type=str, choices=['parallel', 'serial', 'both'], 
                        default='parallel', help='WAN测试模式: 并行/串行/对比测试')
    parser.add_argument('--test-count', type=int, default=3, 
                        help='测试次数，用于找出最快WAN口时')
    parser.add_argument('--mode', type=str, choices=['recent', 'date', 'range', 'full'], 
                        default='recent', help='数据获取模式')
    parser.add_argument('--date', type=str, help='指定日期 (YYYYMMDD格式)')
    parser.add_argument('--start-date', type=str, help='起始日期 (YYYYMMDD格式)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYYMMDD格式)')
    
    return parser.parse_args()


def main():
    """主函数"""
    # 设置日志
    logger = setup_logging()
    logger.info("QuantDB 系统启动")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 检查系统状态
    if args.check:
        status = check_system_status()
        all_ok = all(status.values())
        logger.info(f"系统状态检查: {'正常' if all_ok else '异常'}")
        for component, status in status.items():
            logger.info(f"  - {component}: {'正常' if status else '异常'}")
    
    # 验证多WAN配置
    if args.validate_wan:
        logger.info("验证多WAN配置...")
        
        # 确保网络管理器已初始化
        if network_manager:
            # 刷新接口状态
            network_manager.refresh_interfaces()
            
            # 验证多WAN配置
            result = network_manager.validate_multi_wan()
            
            logger.info(f"多WAN验证结果: {result['status']}")
            logger.info(f"消息: {result['message']}")
            
            if result['status'] == 'valid' or result['status'] == 'invalid':
                logger.info("接口详情:")
                for interface in result['interfaces']:
                    logger.info(f"  - {interface['name']} (内部IP: {interface['source_ip']}, 外部IP: {interface['external_ip']})")
            
            if not result['validation']:
                logger.warning("多WAN验证失败，请检查网络配置")
        else:
            logger.error("网络管理器未初始化，无法验证多WAN配置")
            
    # 测试多WAN接口
    if args.test_wan:
        logger.info("开始测试多WAN接口...")
        
        # 确保网络管理器已初始化
        if network_manager:
            # 根据参数选择测试模式
            wan_mode = args.wan_mode.lower()
            
            if wan_mode == 'both':
                # 执行对比测试
                test_wan_comparison()
            elif wan_mode == 'serial':
                # 串行测试
                test_wan_parallel(parallel=False)
            else:
                # 并行测试（默认）
                test_wan_parallel(parallel=True)
        else:
            logger.error("网络管理器未初始化，无法测试WAN接口")
    
    # 寻找最快的WAN口
    if args.find_fastest_wan:
        logger.info("开始寻找最快的WAN口...")
        
        # 确保网络管理器已初始化
        if network_manager:
            # 创建TushareWANTester实例
            tester = TushareWANTester()
            
            # 获取测试次数
            test_count = max(1, args.test_count)
            
            # 寻找最快WAN口
            fastest_wan = tester.find_fastest_wan(test_count=test_count)
            
            if fastest_wan:
                logger.info(f"找到最快的WAN口: WAN {fastest_wan['wan_idx']} ({fastest_wan['name']})")
                logger.info(f"平均耗时: {fastest_wan['average_time']:.3f}秒, 成功率: {fastest_wan['success_rate']}")
                logger.info(f"外部IP: {fastest_wan['external_ip']}")
            else:
                logger.error("未能找到最快的WAN口，请检查网络状态")
        else:
            logger.error("网络管理器未初始化，无法测试WAN接口")
    
    # 通过最快WAN口发送API请求
    if args.api_request:
        logger.info("通过最快WAN口发送API请求...")
        
        # 确保网络管理器已初始化
        if network_manager:
            # 处理API参数
            api_name = args.api_name
            
            # 解析JSON参数
            params = None
            if args.api_params:
                try:
                    import json
                    params = json.loads(args.api_params)
                    logger.info(f"解析API参数: {params}")
                except Exception as e:
                    logger.error(f"解析API参数失败: {str(e)}")
                    logger.error("请确保参数是有效的JSON格式，例如: '{\"exchange\":\"\",\"start_date\":\"20230101\",\"end_date\":\"20230110\"}'")
                    return
            
            # 发送请求
            result = api_request_via_fastest_wan(api_name, params)
            
            # 处理结果
            if result['success']:
                df = result['data']
                rows = result['rows']
                elapsed = result['elapsed']
                wan_idx = result['wan_idx']
                
                logger.info(f"API请求成功, 通过WAN {wan_idx} 获取了 {rows} 行数据, 耗时: {elapsed:.3f}秒")
                
                # 显示数据前10行
                if df is not None and not df.empty:
                    logger.info(f"数据预览 (前10行):\n{df.head(10)}")
            else:
                logger.error(f"API请求失败: {result.get('error', '未知错误')}")
        else:
            logger.error("网络管理器未初始化，无法执行API请求")
    
    # 运行示例数据获取器
    if args.demo:
        logger.info("运行示例数据获取器")
        fetcher = ExampleFetcher()
        
        # 根据模式运行
        if args.mode == 'recent':
            result = fetcher.run(mode='recent')
        elif args.mode == 'date':
            if not args.date:
                logger.error("缺少日期参数，请使用 --date 指定日期")
                return
            result = fetcher.run(mode='date', date=args.date)
        elif args.mode == 'range':
            if not args.start_date or not args.end_date:
                logger.error("缺少日期范围参数，请使用 --start-date 和 --end-date 指定日期范围")
                return
            result = fetcher.run(mode='range', start_date=args.start_date, end_date=args.end_date)
        elif args.mode == 'full':
            result = fetcher.run(mode='full')
        
        # 输出结果
        if result.get('status') == 'success':
            logger.info(f"示例运行成功: {result.get('message')}")
            logger.info(f"保存记录数: {result.get('saved_count', 0)}")
            logger.info(f"执行时间: {result.get('execution_time', 0):.2f} 秒")
        else:
            logger.error(f"示例运行失败: {result.get('message')}")
    
    logger.info("QuantDB 系统运行完成")


if __name__ == "__main__":
    main()