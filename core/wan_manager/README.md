# WAN端口管理模块

本模块提供了多WAN接口的端口分配和管理功能，支持跨进程安全的端口分配、释放和监控。

## 主要组件

### WanPortPool - WAN端口池

新的WAN端口池实现，提供跨进程安全的端口管理：

```python
from core.wan_manager import wan_port_pool

# 获取端口
port = wan_port_pool.allocate_port(wan_idx=0)

# 使用端口进行网络操作
...

# 释放端口
wan_port_pool.release_port(wan_idx=0, port=port)
```

特点：
- 单例模式实现，确保应用内共享同一实例
- 使用文件锁实现跨进程同步
- 自动跟踪端口分配状态和进程使用情况
- 定期清理超时未释放的端口
- 程序退出时自动释放分配的所有端口

### 旧API兼容 - 通过适配器层

为保证兼容性，提供了与旧版`port_allocator`接口兼容的适配器：

```python
# 这个导入方式与旧代码完全相同
from core.wan_manager import port_allocator

# 使用旧的API，但底层使用新的WanPortPool实现
port = port_allocator.allocate_port(wan_idx=0)
port_allocator.release_port(wan_idx=0, port=port)
```

## 实现原理

WanPortPool采用以下关键技术确保在多进程环境下的安全操作：

1. **进程间同步**：使用文件锁（fcntl）确保多个进程间安全访问共享状态
2. **状态持久化**：将分配状态保存在JSON文件中，支持多进程共享访问
3. **进程监控**：跟踪分配端口的进程ID，进程退出时自动释放资源
4. **超时清理**：定期检查长时间未释放的端口，并自动释放不再使用的资源

## 典型使用场景

### 在并行任务环境中

gorepe.py并行执行多个数据获取任务时，可以避免端口冲突：

```python
# 在daily_basic_fetcher2.py中
from core.wan_manager import port_allocator

# 这里port_allocator实际上是使用WanPortPool实现的适配器
# 多个进程同时调用时，会通过文件锁确保端口分配的安全
port = port_allocator.allocate_port(wan_idx)
```

### 直接使用WanPortPool

如果需要更高级的功能，可以直接使用WanPortPool：

```python
from core.wan_manager import WanPortPool

# 获取单例实例
pool = WanPortPool.get_instance()

# 获取端口使用情况
usage_info = pool.get_port_usage_info()
print(f"当前已分配端口数: {usage_info['total_allocated_ports']}")

# 分配端口
port = pool.allocate_port(wan_idx=0)

# 释放端口
pool.release_port(wan_idx=0, port=port)
``` 