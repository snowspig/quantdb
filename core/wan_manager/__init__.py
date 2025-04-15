#!/usr/bin/env python
"""
WAN管理模块
提供多WAN接口的端口池和负载均衡管理
"""

# 系统导入
import time
import platform
import logging
import threading
import socket
from typing import List, Dict

# 标记初始化状态
_initialized = False
_init_lock = threading.Lock()
_init_in_progress = False
_init_timeout = 10  # 超时时间（秒）

# 预先声明组件变量，稍后再导入
# 这样可以避免在导入时自动初始化
wan_port_pool = None
port_allocator_adapter = None
wan_monitor = None
load_balancer = None
port_allocator = None

# 获取日志记录器
logger_init = logging.getLogger("core.wan_manager.init")

def _lazy_initialize():
    """延迟初始化WAN管理模块，确保单例模式和依赖组件的可用性"""
    global _initialized, _init_in_progress
    global wan_port_pool, port_allocator_adapter, wan_monitor, load_balancer, port_allocator
    
    # 如果已经初始化或正在初始化中，直接返回
    if _initialized or _init_in_progress:
        logger_init.debug(f"_lazy_initialize called but already initialized ({_initialized}) or in progress ({_init_in_progress}). Skipping.")
        return
    
    with _init_lock:
        # 双重检查锁
        if _initialized or _init_in_progress:
            logger_init.debug(f"_lazy_initialize (in lock) called but already initialized ({_initialized}) or in progress ({_init_in_progress}). Skipping.")
            return
            
        # 标记为正在初始化
        _init_in_progress = True
        init_start_time = time.time()
        logger_init.info("Starting WAN manager lazy initialization...")
        
        try:
            # 超时控制
            start_time = time.time()
            
            logger_init.debug("Importing component classes...")
            from .wan_port_pool import WanPortPool # 只导入类
            from .port_allocator_adapter import PortAllocatorAdapter # 只导入类
            logger_init.debug("Base component classes imported.")
            
            # 检查是否超时
            if time.time() - start_time > _init_timeout/2:
                logger_init.warning(f"WAN管理模块导入类耗时过长: {time.time() - start_time:.2f}秒")
                # 强制继续
            
            # 实例化组件
            logger_init.debug("Instantiating components...")
            # 使用 get_instance() 获取 WanPortPool 单例
            wan_port_pool = WanPortPool.get_instance() 
            logger_init.debug(f"Instantiated wan_port_pool: {wan_port_pool}")
            # 创建 PortAllocatorAdapter 实例
            port_allocator_adapter = PortAllocatorAdapter()
            logger_init.debug(f"Instantiated port_allocator_adapter: {port_allocator_adapter}")
            # 设置别名
            port_allocator = port_allocator_adapter
            logger_init.debug(f"Set port_allocator alias: {port_allocator}")
            
            # 尝试导入并实例化监控器和负载均衡器
            try:
                logger_init.debug("Importing and instantiating optional components (WANMonitor, LoadBalancer)...")
                from .wan_monitor import WANMonitor # 只导入类
                # wan_monitor = WANMonitor.get_instance() # 不再尝试 get_instance
                wan_monitor = WANMonitor() # 直接调用构造函数
                logger_init.debug(f"Instantiated wan_monitor: {wan_monitor}")
                
                from .load_balancer import LoadBalancer # 只导入类
                # load_balancer = LoadBalancer.get_instance() # 不再尝试 get_instance
                load_balancer = LoadBalancer() # 直接调用构造函数
                logger_init.debug(f"Instantiated load_balancer: {load_balancer}")
                
            except ImportError as e:
                logger_init.warning(f"无法导入可选组件类: {str(e)}，相关功能将不可用")
                wan_monitor = None
                load_balancer = None
            # 移除检查 get_instance 的 AttributeError 处理
            # except AttributeError as e:
            #    logger_init.warning(f"可选组件类缺少 get_instance() 方法: {str(e)}，尝试直接实例化")
            #    ...
            except Exception as e:
                logger_init.warning(f"导入或实例化监控器/负载均衡器失败: {str(e)}，将使用 None", exc_info=True)
                wan_monitor = None
                load_balancer = None

            logger_init.debug("Components instantiated.")

            # 监控器启动使用非阻塞方式
            try:
                if wan_monitor and hasattr(wan_monitor, 'start_monitoring'):
                    logger_init.debug("启动WAN监控 (后台线程)")
                    def start_monitor():
                        try:
                            wan_monitor.start_monitoring()
                        except Exception as e:
                            logger_init.error(f"启动WAN监控失败: {str(e)}")
                    
                    monitor_thread = threading.Thread(target=start_monitor, daemon=True, name="WANMonitorThread")
                    monitor_thread.start()
            except Exception as e:
                logger_init.error(f"创建或启动WAN监控线程失败: {str(e)}")
            
            # 尝试初始化负载均衡器，但不阻塞
            try:
                if load_balancer and hasattr(load_balancer, 'initialize'):
                    logger_init.debug("初始化负载均衡器")
                    init_timeout_lb = threading.Timer(_init_timeout/4, lambda: logging.warning("负载均衡器初始化超时"))
                    init_timeout_lb.start()
                    load_balancer.initialize()
                    init_timeout_lb.cancel()
            except Exception as e:
                logger_init.error(f"负载均衡器初始化失败: {str(e)}")
            
            # 检查总耗时
            elapsed = time.time() - start_time
            if elapsed > _init_timeout:
                logger_init.warning(f"WAN管理模块初始化耗时过长: {elapsed:.2f}秒")
            else:
                logger_init.info(f"WAN管理模块初始化完成，耗时 {elapsed:.2f}秒")

            # 标记为已初始化
            _initialized = True
            logger_init.info("WAN manager lazy initialization marked as complete (_initialized = True).")
            
        except Exception as e:
            logger_init.error(f"WAN管理模块初始化失败: {str(e)}", exc_info=True)
            # 创建默认值，确保模块可用
            if wan_port_pool is None:
                logger_init.warning("创建默认WAN端口池 due to failure.")
                class DummyPool: # 内联定义，避免导入失败时再出问题
                    def get_instance(cls): return cls()
                    def allocate_port(self, *args, **kwargs): return None
                    def release_port(self, *args, **kwargs): return False
                    def get_available_wan_indices(self): return []
                wan_port_pool = DummyPool()
                logger_init.debug(f"Assigned DummyPool to wan_port_pool: {wan_port_pool}")
                
            if port_allocator_adapter is None:
                logger_init.warning("创建默认端口分配适配器 due to failure.")
                class DummyAdapter: # 内联定义
                    def __init__(self, *args, **kwargs): pass # 接受可能传递的参数
                    def allocate_port(self, *args, **kwargs): return None
                    def release_port(self, *args, **kwargs): return False
                    def get_available_wan_indices(self): return []
                port_allocator_adapter = DummyAdapter()
                port_allocator = port_allocator_adapter # 确保别名也设置
                logger_init.debug(f"Assigned DummyAdapter to port_allocator_adapter: {port_allocator_adapter}")
                logger_init.debug(f"Assigned DummyAdapter to port_allocator: {port_allocator}")
                
            # 确保其他组件在失败时为None
            if wan_monitor is None: logger_init.debug("wan_monitor remains None after failure.")
            if load_balancer is None: logger_init.debug("load_balancer remains None after failure.")

            # 标记为已初始化，即使失败也避免重复尝试
            _initialized = True
            logger_init.warning("WAN manager lazy initialization marked as complete after failure (_initialized = True).")
        finally:
            # 无论成功与否，都标记初始化过程已结束
            _init_in_progress = False
            # 检查总初始化时间
            total_elapsed = time.time() - init_start_time
            logger_init.debug(f"WAN manager lazy initialization process finished. Total time: {total_elapsed:.2f}s")

# 获取协调器的方法
def get_coordinator():
    """懒加载获取协调器实例"""
    # 确保基础组件已初始化
    _ensure_components()
    
    # 延迟导入协调器
    from .wan_coordinator import WanNetworkCoordinator
    return WanNetworkCoordinator.get_instance()

# 确保组件已加载的辅助函数
def _ensure_components():
    """确保基础组件已经加载"""
    global _initialized
    # 检查 _initialized 标志而不是具体组件
    if not _initialized:
        logger_init.debug("_ensure_components triggered: Running lazy initialization.")
        _lazy_initialize()
    # else: # 可以取消注释以进行调试
    #    logger_init.debug("_ensure_components triggered: Already initialized.")

# 添加便捷函数获取端口分配器
def get_port_allocator():
    """获取端口分配器"""
    _ensure_components()
    logger_init.debug(f"get_port_allocator called. Returning: {port_allocator_adapter}")
    return port_allocator_adapter

# 添加便捷函数获取WAN端口池
def get_wan_port_pool():
    """获取WAN端口池"""
    _ensure_components()
    return wan_port_pool

# 添加便捷函数获取WAN监控器
def get_wan_monitor():
    """获取WAN监控器"""
    _ensure_components()
    logger_init.debug(f"get_wan_monitor called. Returning: {wan_monitor}")
    return wan_monitor

# 添加便捷函数获取负载均衡器
def get_load_balancer():
    """获取负载均衡器"""
    _ensure_components()
    return load_balancer

# 便捷函数
def is_windows() -> bool:
    """检查当前系统是否为Windows"""
    return platform.system().lower() == 'windows'

def generate_optimization_suggestions() -> Dict:
    """
    生成Windows网络优化建议
    
    Returns:
        Dict: 优化建议字典
    """
    from .win_socket_optimizer import generate_optimization_suggestions as gen_opt
    return gen_opt()

def get_network_interfaces() -> List[Dict]:
    """
    获取系统网络接口列表
    
    Returns:
        List[Dict]: 网络接口列表
    """
    # 此功能实际依赖于wan_monitor的实现
    interfaces = []
    try:
        # 通过wan_monitor获取网络接口信息
        if wan_monitor:
            available_wans = wan_monitor.get_available_wans()
            for wan_idx in available_wans:
                interfaces.append({
                    "wan_idx": wan_idx,
                    "status": "active" if wan_monitor.is_wan_available(wan_idx) else "inactive"
                })
    except Exception as e:
        logging.error(f"获取网络接口列表失败: {str(e)}")
    return interfaces

def check_network_connectivity(test_url: str = "http://106.14.185.239:29990/test") -> Dict:
    """
    检查网络连接状态
    
    Args:
        test_url: 测试URL
        
    Returns:
        Dict: 连接测试结果
    """
    from .utils import test_connection
    results = {}
    
    try:
        # 解析URL获取主机和端口
        import urllib.parse
        parsed = urllib.parse.urlparse(test_url)
        host = parsed.netloc.split(':')[0]
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        
        # 对每个WAN接口进行测试
        available_wans = wan_monitor.get_available_wans() if wan_monitor else []
        for wan_idx in available_wans:
            allocated_port = None
            try:
                # 分配本地端口
                allocated_port = wan_port_pool.allocate_port(wan_idx)
                if allocated_port:
                    # 使用分配的端口进行连接测试
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    sock.bind(('0.0.0.0', allocated_port))
                    
                    # 记录测试开始时间
                    start_time = time.time()
                    sock.connect((host, port))
                    
                    # 计算连接时间
                    connection_time = (time.time() - start_time) * 1000  # 毫秒
                    sock.close()
                    
                    results[wan_idx] = {
                        "status": "connected",
                        "time_ms": round(connection_time, 2)
                    }
                else:
                    results[wan_idx] = {
                        "status": "error",
                        "message": "无法分配端口"
                    }
            except Exception as e:
                results[wan_idx] = {
                    "status": "failed",
                    "message": str(e)
                }
            finally:
                # 释放端口
                if allocated_port:
                    wan_port_pool.release_port(wan_idx, allocated_port)
    except Exception as e:
        logging.error(f"网络连接测试失败: {str(e)}")
        results["error"] = str(e)
    
    return results

def optimize_network_settings() -> Dict:
    """
    优化网络设置
    
    Returns:
        Dict: 优化结果
    """
    if is_windows():
        from .win_socket_optimizer import apply_optimizations
        return apply_optimizations()
    else:
        return {"success": False, "message": "仅支持Windows系统"}

# 修改__all__列表，使用函数替代直接导出实例
__all__ = [
    # 函数获取组件
    'get_port_allocator',
    'get_wan_port_pool',
    'get_wan_monitor',
    'get_load_balancer',
    'get_coordinator',
    
    # 向后兼容的别名
    'port_allocator',
    
    # 工具函数
    'is_windows',
    'is_admin',
    'generate_optimization_suggestions',
    'get_network_interfaces',
    'check_network_connectivity',
    'optimize_network_settings',
    'get_system_info',
    '_lazy_initialize',
    '_ensure_components',
    
    # 类型定义 - 改为按需导入，避免立即加载模块
    # 'WanPortPool',
    # 'PortAllocatorAdapter',
    # 'WANMonitor',
    # 'LoadBalancer',
    # 'WanNetworkCoordinator',
]

# 版本信息
__version__ = '0.3.0'

def _check_system_optimization():
    """检查系统优化状态并在日志中提供建议"""
    if not is_windows():
        return
    
    try:
        # 延迟几秒再检查，避免影响应用启动速度
        time.sleep(5)
        
        # 获取优化建议
        try:
            from .win_socket_optimizer import generate_optimization_suggestions as gen_opt
            opt_suggestions = gen_opt()
        except Exception as e:
            logging.warning(f"无法获取系统优化建议: {str(e)}")
            return

        if not opt_suggestions.get('supported', False):
            return
            
        # 如果有优化建议，打印到日志
        if opt_suggestions.get('suggestions', []):
            logger = logging.getLogger("core.wan_manager")
            logger.warning("发现Windows TCP/IP设置可优化项:")
            for i, suggestion in enumerate(opt_suggestions.get('suggestions', []), 1):
                logger.warning(f"  {i}. {suggestion}")
            
            logger.warning(
                "为提高并发网络性能，建议运行 'python -m core.wan_manager.win_socket_optimizer' "
                "查看详细优化建议并应用这些优化"
            )
    except Exception as e:
        logging.error(f"检查系统优化状态时出错: {str(e)}")

# 确保使用弱引用和异常捕获方式运行这部分
try:
    # 在后台线程中执行检查，避免阻塞应用启动
    # 确保线程为守护线程，且不会阻塞主线程
    # startup_thread = threading.Thread(target=_lazy_initialize, daemon=True, name="WanManager-Startup")
    # startup_thread.start()
    
    # 添加系统优化检查线程，但不要让它影响主线程
    # optimization_thread = threading.Thread(target=_check_system_optimization, daemon=True, name="WanManager-OptCheck")
    # optimization_thread.start()
    pass # 添加pass避免空try块
except Exception as e:
    logging.error(f"启动WAN管理模块初始化线程失败: {str(e)}")
    # 确保不论如何都将_initialized和_init_in_progress重置
    _initialized = False
    _init_in_progress = False 