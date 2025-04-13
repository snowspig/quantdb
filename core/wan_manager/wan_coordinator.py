"""
WAN网络协调器 - 统一管理网络资源分配和请求执行

该模块提供了一个协调器类，用于统一管理WAN端口分配、负载均衡和请求执行，
简化了多WAN环境下的网络请求流程，提供更好的错误处理和资源管理。
"""
import time
import logging
import threading
from typing import Dict, List, Any,  Callable

# 导入WAN管理组件
from .utils import retry


class WanNetworkCoordinator:
    """WAN网络协调器 - 统一管理端口分配、负载均衡和监控"""
    
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls):
        """获取单例实例"""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """初始化WAN网络协调器"""
        # 延迟绑定组件，减少导入时的资源消耗
        self._port_pool = None
        self._load_balancer = None
        self._monitor = None
        self.logger = logging.getLogger("core.wan_manager.Coordinator")
        
        # 资源跟踪
        self.active_resources = {}
        self.resources_lock = threading.Lock()
        
        # 请求统计
        self.request_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'avg_response_time': 0
        }
        self.stats_lock = threading.Lock()
        
    @property
    def port_pool(self):
        """懒加载端口池"""
        if self._port_pool is None:
            from . import get_wan_port_pool
            self._port_pool = get_wan_port_pool()
        return self._port_pool
    
    @property
    def load_balancer(self):
        """懒加载负载均衡器"""
        if self._load_balancer is None:
            from . import get_load_balancer
            self._load_balancer = get_load_balancer()
        return self._load_balancer
    
    @property
    def monitor(self):
        """懒加载监控器"""
        if self._monitor is None:
            from . import get_wan_monitor
            self._monitor = get_wan_monitor()
        return self._monitor
    
    def allocate_resource(self, wan_idx=None, request_type="api") -> Dict:
        """
        分配网络资源（WAN索引和端口）
        
        Args:
            wan_idx: 可选的指定WAN索引，如果不指定则由负载均衡器选择
            request_type: 请求类型，用于区分不同资源用途
            
        Returns:
            Dict: 包含资源信息的字典，包括wan_idx, port和resource_id
            
        Raises:
            ValueError: 当无法分配资源时抛出
        """
        # 如果未指定WAN索引，使用负载均衡选择
        if wan_idx is None:
            wan_idx = self.load_balancer.select_wan()
            
        if wan_idx < 0:
            raise ValueError("无可用WAN接口")
            
        # 分配端口
        port = self.port_pool.allocate_port(wan_idx)
        if not port:
            raise ValueError(f"WAN {wan_idx} 无可用端口")
            
        # 生成唯一资源ID
        resource_id = f"{wan_idx}:{port}"
        
        # 记录资源分配
        with self.resources_lock:
            self.active_resources[resource_id] = {
                "wan_idx": wan_idx,
                "port": port,
                "allocated_time": time.time(),
                "request_type": request_type,
                "health_score": self.monitor.health_scores.get(wan_idx, 0) if hasattr(self.monitor, 'health_scores') else 0
            }
            
        self.logger.debug(f"已分配资源: {resource_id} (WAN {wan_idx}, 端口 {port})")
        
        return {
            "wan_idx": wan_idx,
            "port": port,
            "health_score": self.active_resources[resource_id]["health_score"],
            "resource_id": resource_id
        }
        
    def release_resource(self, resource_id) -> bool:
        """
        释放网络资源
        
        Args:
            resource_id: 资源ID (格式为 "wan_idx:port")
            
        Returns:
            bool: 是否成功释放资源
        """
        try:
            if not resource_id or ':' not in resource_id:
                return False
                
            wan_idx, port = map(int, resource_id.split(':'))
            
            # 从活动资源中移除
            with self.resources_lock:
                if resource_id in self.active_resources:
                    del self.active_resources[resource_id]
            
            # 释放端口
            result = self.port_pool.release_port(wan_idx, port)
            
            if result:
                self.logger.debug(f"已释放资源: {resource_id}")
            else:
                self.logger.warning(f"释放资源失败: {resource_id}")
                
            return result
            
        except Exception as e:
            self.logger.error(f"释放资源时发生异常: {str(e)}")
            return False
            
    def execute_request(self, callback: Callable, resource=None, retry_count=3, 
                       retry_delay=1.0, **kwargs) -> Any:
        """
        执行网络请求，自动处理资源分配和错误恢复
        
        Args:
            callback: 执行请求的回调函数，接收resource参数
            resource: 可选的预分配资源，如果未提供则自动分配
            retry_count: 重试次数
            retry_delay: 重试延迟（秒）
            **kwargs: 传递给回调函数的额外参数
            
        Returns:
            Any: 回调函数的返回值
            
        Raises:
            Exception: 当所有重试都失败时抛出
        """
        allocated_resource = False
        resource_info = resource
        result = None
        last_error = None
        
        # 更新请求计数
        with self.stats_lock:
            self.request_stats['total_requests'] += 1
        
        # 定义重试装饰的函数
        @retry(max_attempts=retry_count, delay=retry_delay, logger=self.logger)
        def _execute_with_retry():
            nonlocal allocated_resource, resource_info
            
            # 如果没有提供资源，自动分配
            if not resource_info:
                resource_info = self.allocate_resource(**kwargs.get('resource_options', {}))
                allocated_resource = True
                
            # 复制kwargs以避免修改原始参数
            request_kwargs = kwargs.copy()
            request_kwargs['resource'] = resource_info
            
            start_time = time.time()
            try:
                # 执行回调函数
                result = callback(**request_kwargs)
                elapsed = time.time() - start_time
                
                # 记录成功
                self.load_balancer.record_success(resource_info["wan_idx"], elapsed)
                
                # 更新统计信息
                with self.stats_lock:
                    self.request_stats['successful_requests'] += 1
                    # 更新平均响应时间
                    current_avg = self.request_stats['avg_response_time']
                    success_count = self.request_stats['successful_requests']
                    if success_count > 1:
                        self.request_stats['avg_response_time'] = ((current_avg * (success_count - 1)) + elapsed) / success_count
                    else:
                        self.request_stats['avg_response_time'] = elapsed
                        
                return result
                
            except Exception as e:
                # 记录失败
                self.load_balancer.record_error(resource_info["wan_idx"])
                
                # 更新统计信息
                with self.stats_lock:
                    self.request_stats['failed_requests'] += 1
                
                # 如果是自动分配的资源，尝试释放
                if allocated_resource:
                    self.release_resource(resource_info["resource_id"])
                    allocated_resource = False  # 重置状态，下次重试时会重新分配
                    resource_info = None
                
                self.logger.warning(f"请求执行失败 (WAN {resource_info['wan_idx'] if resource_info else 'unknown'}): {str(e)}")
                raise  # 重新抛出异常，以触发重试机制
        
        try:
            # 执行带重试的函数
            return _execute_with_retry()
            
        finally:
            # 确保资源被释放
            if allocated_resource and resource_info:
                self.release_resource(resource_info["resource_id"])
                
    def batch_execute(self, callback: Callable, items: List[Any], max_workers=None, 
                     **kwargs) -> List[Dict]:
        """
        批量执行请求，自动处理并行和资源分配
        
        Args:
            callback: 执行请求的回调函数
            items: 要处理的项目列表
            max_workers: 最大工作线程数，默认为可用WAN接口数量
            **kwargs: 传递给回调函数的额外参数
            
        Returns:
            List[Dict]: 每个请求的结果列表
        """
        if not items:
            return []
            
        # 设置最大工作线程数
        available_wans = self.monitor.get_available_wans()
        if max_workers is None:
            max_workers = len(available_wans) if available_wans else 4
            max_workers = min(max_workers, len(items), 10)  # 限制最大线程数
            
        self.logger.info(f"批量执行 {len(items)} 个请求，使用 {max_workers} 个工作线程")
        
        results = []
        finished_count = 0
        lock = threading.Lock()
        
        # 定义工作函数
        def worker(item_index):
            nonlocal finished_count
            item = items[item_index]
            
            try:
                # 为每个请求分配资源并执行
                item_kwargs = kwargs.copy()
                item_kwargs['item'] = item
                item_kwargs['item_index'] = item_index
                
                # 执行请求
                result = self.execute_request(callback, **item_kwargs)
                
                item_result = {
                    'success': True,
                    'item_index': item_index,
                    'result': result,
                    'error': None
                }
            except Exception as e:
                self.logger.error(f"处理项目 {item_index} 时发生异常: {str(e)}")
                item_result = {
                    'success': False,
                    'item_index': item_index,
                    'result': None,
                    'error': str(e)
                }
                
            # 更新进度
            with lock:
                results.append(item_result)
                finished_count += 1
                if finished_count % 10 == 0 or finished_count == len(items):
                    self.logger.info(f"批量进度: {finished_count}/{len(items)} 完成")
                    
            return item_result
            
        # 创建并启动工作线程
        threads = []
        for i in range(len(items)):
            thread = threading.Thread(target=worker, args=(i,))
            thread.daemon = True
            threads.append(thread)
            thread.start()
            
            # 控制并发度
            if len(threads) >= max_workers:
                threads[0].join()
                threads.pop(0)
                
        # 等待所有线程完成
        for thread in threads:
            thread.join()
            
        # 按原始索引排序结果
        results.sort(key=lambda x: x['item_index'])
        
        # 统计成功率
        success_count = sum(1 for r in results if r['success'])
        self.logger.info(f"批量执行完成: {success_count}/{len(results)} 成功 ({success_count/len(results)*100:.1f}%)")
        
        return results
        
    def get_stats(self) -> Dict:
        """
        获取资源和请求统计信息
        
        Returns:
            Dict: 统计信息
        """
        with self.stats_lock:
            stats = self.request_stats.copy()
            
        with self.resources_lock:
            stats['active_resources'] = len(self.active_resources)
            
        # 添加负载均衡和监控相关统计
        stats['load_balancer'] = self.load_balancer.get_stats()
        
        # WAN状态
        stats['available_wans'] = len(self.monitor.get_available_wans())
        
        return stats
        
    def health_check(self) -> Dict:
        """
        执行系统健康检查
        
        Returns:
            Dict: 健康状态信息
        """
        health_result = {
            'status': 'healthy',
            'checks': {},
            'timestamp': time.time()
        }
        
        # 检查WAN接口可用性
        available_wans = self.monitor.get_available_wans()
        wan_check = {
            'status': 'pass' if available_wans else 'fail',
            'message': f"{len(available_wans)} 个WAN接口可用" if available_wans else "无可用WAN接口",
            'available_wans': len(available_wans)
        }
        health_result['checks']['wan_availability'] = wan_check
        
        # 检查端口池状态
        try:
            port_usage = self.port_pool.get_port_usage_info()
            port_check = {
                'status': 'pass',
                'message': f"端口池正常，已分配 {port_usage['total_allocated_ports']} 个端口",
                'allocated_ports': port_usage['total_allocated_ports']
            }
        except Exception as e:
            port_check = {
                'status': 'fail',
                'message': f"端口池异常: {str(e)}"
            }
        health_result['checks']['port_pool'] = port_check
        
        # 资源泄漏检查
        with self.resources_lock:
            stale_resources = []
            current_time = time.time()
            for res_id, res_info in list(self.active_resources.items()):
                # 检查资源是否超过5分钟未释放
                if current_time - res_info['allocated_time'] > 300:
                    stale_resources.append(res_id)
                    
            leak_check = {
                'status': 'warning' if stale_resources else 'pass',
                'message': f"发现 {len(stale_resources)} 个疑似泄漏资源" if stale_resources else "无资源泄漏",
                'stale_resources': len(stale_resources)
            }
            
            # 如果发现疑似泄漏资源，尝试清理
            if stale_resources:
                for res_id in stale_resources[:]:
                    if self.release_resource(res_id):
                        stale_resources.remove(res_id)
                leak_check['cleaned_resources'] = len(stale_resources)
                
        health_result['checks']['resource_leaks'] = leak_check
        
        # 整体状态判断
        if any(check['status'] == 'fail' for check in health_result['checks'].values()):
            health_result['status'] = 'unhealthy'
        elif any(check['status'] == 'warning' for check in health_result['checks'].values()):
            health_result['status'] = 'warning'
            
        return health_result


