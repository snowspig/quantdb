import os
import multiprocessing
import logging
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import time
import yaml
import subprocess
import signal

# 设置进程数
TOTAL_WORKERS = min(12, multiprocessing.cpu_count() - 2)
SERIAL_WORKERS = 1
PARALLEL_WORKERS = TOTAL_WORKERS - SERIAL_WORKERS

os.environ["MAX_WORKERS"] = str(TOTAL_WORKERS)

def signal_handler(signum, frame):
    """处理中断信号"""
    logging.info("收到中断信号，正在优雅关闭...")
    raise KeyboardInterrupt

def load_config():
    """从配置文件加载任务列表"""
    config_path = 'tasks_config.yaml'
    if not os.path.exists(config_path):
        # 如果配置文件不存在，创建默认配置
        default_config = {
            'init_programs': [
                'trade_cal_fetcher.py',
                'stock_basic_fetcher.py'
            ],
            'serial_programs': [
                'wan_test_client.py'            
                ],
            'parallel_programs': [
                'suspend_fetcher.py',
                'stk_rewards_fetcher.py',
                'stock_company_fetcher.py',
                'stock_managers_fetcher.py',
                'stock_previous_name_fetcher.py',
                'daily_fetcher.py',
                'div_fetcher.py',
                'weekly_fetcher.py',
                'monthly_fetcher.py'
                
                
                # 'miss_adj_factor.py',
                # 'miss_daily_basic_ts.py',
                # 'miss_daily.py',
                # 'miss_div.py',
                # 'miss_ggt_daily.py',
                # 'miss_ggt_monthly.py',
                # 'miss_ggt_sh_daily.py',
                # 'miss_hk_hold.py',
                # 'miss_hsgt_top10.py',
                # 'miss_limit_list_d.py',
                # 'miss_moneyflow_hsgt.py',
                # 'miss_moneyflow.py',
                # 'miss_monthly.py',
                # 'miss_stk_limit.py',
                # 'miss_suspend.py',
                # 'miss_weekly.py',
                # 'get_income.py',
                # 'get_balancesheet.py',                  
                # 'get_cashflow.py',
                # 'get_forecast.py',
                # 'get_express.py',
                # 'get_balancesheet.py',  
                # 'get_fin_indicator_basic.py',                  
                # 'get_fin_indicator.py', 
                # 'get_fina_audit.py',
                # 'get_fin_mainbz.py',                
                # 'get_dividend.py',
                # 'get_margin.py',
                # 'get_margin_detail.py',                
                # 'get_disclosure_date.py',
                # 'get_stock_company.py',
                # 'get_previous_name.py',  
                # 'get_ggt_top10.py',
                # 'get_top10_holders.py',
                # 'get_top10_floatholders.py',
                # 'get_top_list.py',
                # 'get_top_inst.py',
                # 'get_pledge_stat.py',
                # 'get_suspend.py',
                # 'get_pledge_detail.py',
                # 'get_repurchase.py',
                # 'get_concept.py',

            ]
        }
        with open(config_path, 'w', encoding='utf-8') as f:
            yaml.dump(default_config, f, allow_unicode=True)
        
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def execute_file(file_path):
    """执行单个Python文件"""
    try:
        logging.info(f"开始执行: {file_path}")
        process = subprocess.Popen(
            ['python', file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # 设置超时时间（例如2小时）
        try:
            stdout, stderr = process.communicate(timeout=7200)
            if stdout:
                logging.info(stdout)
            if stderr:
                logging.warning(stderr)
            if process.returncode == 0:
                logging.info(f"执行完成: {file_path}")
            else:
                logging.error(f"执行失败: {file_path}, 返回码: {process.returncode}")
        except subprocess.TimeoutExpired:
            process.kill()
            logging.error(f"{file_path} 执行超时")
            raise
            
    except Exception as e:
        logging.error(f"执行{file_path}时出错: {e}")
        raise

def main():
    """主函数"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    config = load_config()
    init_programs = config['init_programs']
    serial_programs = config['serial_programs']
    parallel_programs = config['parallel_programs']
    
    # 先执行初始化程序
    logging.info("\n开始执行初始化程序:")
    for program in init_programs:
        if os.path.exists(program):
            try:
                execute_file(program)
            except Exception as e:
                logging.error(f"初始化程序执行失败: {e}")
                return
        else:
            logging.warning(f"初始化文件不存在: {program}")
            return
    
    # 用字典跟踪任务状态
    task_status = {}
    
    try:
        with ProcessPoolExecutor(max_workers=SERIAL_WORKERS) as serial_executor, \
             ProcessPoolExecutor(max_workers=PARALLEL_WORKERS) as parallel_executor:
            
            # 提交串行任务
            logging.info("\n提交串行任务:")
            serial_futures = {}
            for program in serial_programs:
                if os.path.exists(program):
                    future = serial_executor.submit(execute_file, program)
                    serial_futures[future] = program
                    task_status[program] = 'running'
            
            # 提交并行任务
            logging.info("\n提交并行任务:")
            parallel_futures = {}
            for program in parallel_programs:
                if os.path.exists(program):
                    future = parallel_executor.submit(execute_file, program)
                    parallel_futures[future] = program
                    task_status[program] = 'running'
            
            all_futures = list(serial_futures.keys()) + list(parallel_futures.keys())
            all_programs = {**serial_futures, **parallel_futures}
            
            # 等待所有任务完成，同时处理异常
            while all_futures:
                done, pending = wait(
                    all_futures,
                    return_when=FIRST_COMPLETED,
                    timeout=60
                )
                
                # 处理完成的任务
                for future in done:
                    program_name = all_programs[future]
                    try:
                        future.result()
                        task_status[program_name] = 'completed'
                        logging.info(f"任务完成: {program_name}")
                    except Exception as e:
                        task_status[program_name] = 'failed'
                        logging.error(f"任务失败 {program_name}: {e}")
                
                # 更新待处理任务列表
                all_futures = list(pending)
                
                # 输出详细的任务状态
                if pending:
                    pending_tasks = [all_programs[f] for f in pending]
                    logging.info(f"剩余 {len(pending)} 个任务待完成:")
                    for task in pending_tasks:
                        logging.info(f"- {task}")
                
                # 检查是否所有任务都已完成
                if not pending and not done:
                    logging.info("所有任务已完成，退出循环")
                    break
                    
    except KeyboardInterrupt:
        logging.info("正在终止所有任务...")
    except Exception as e:
        logging.error(f"执行过程中出错: {e}")
    finally:
        # 输出最终任务状态统计
        status_summary = {
            'completed': [],
            'failed': [],
            'running': []
        }
        for program, status in task_status.items():
            status_summary[status].append(program)
            
        logging.info("\n任务执行统计:")
        logging.info(f"完成: {len(status_summary['completed'])} 个")
        logging.info(f"失败: {len(status_summary['failed'])} 个")
        logging.info(f"未完成: {len(status_summary['running'])} 个")
        
        if status_summary['failed']:
            logging.info("\n失败的任务:")
            for program in status_summary['failed']:
                logging.info(f"- {program}")
                
        if status_summary['running']:
            logging.info("\n未完成的任务:")
            for program in status_summary['running']:
                logging.info(f"- {program}")

if __name__ == "__main__":
    main()


