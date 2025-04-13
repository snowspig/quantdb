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

# Windows网络优化工具

## 简介

该工具用于检测和优化Windows系统的TCP/IP设置，特别是对TIME_WAIT状态的持续时间进行优化，以减少端口复用时间，从而降低在高并发情况下的端口冲突问题。

## 主要功能

1. **检测当前TCP参数配置** - 检查并显示当前系统的TCP相关配置参数
2. **提供优化建议** - 根据当前配置，提供针对性的优化建议
3. **自动应用优化** - 在获得管理员权限的情况下，可以自动应用建议的优化配置
4. **系统级优化** - 优化注册表中的TCP参数，如TIME_WAIT延迟、最大用户端口数等

## 使用方法

### 直接运行

```
python -m core.wan_manager.win_socket_optimizer
```

### 在代码中使用

```python
from core.wan_manager.win_socket_optimizer import generate_optimization_suggestions, print_optimization_guide

# 获取优化建议
suggestions = generate_optimization_suggestions()

# 打印优化指南
print_optimization_guide()
```

## 系统要求

- Windows操作系统
- 需要管理员权限才能应用优化设置

## 注意事项

1. 应用优化设置后，建议重启计算机以使更改完全生效
2. 某些设置修改可能会对其他网络应用产生影响，请谨慎操作
3. 如有网络问题，可以使用Windows自带的`netsh int ip reset`命令重置TCP/IP配置

## 优化项目说明

1. **TcpTimedWaitDelay**: 减少TIME_WAIT状态时间，默认值240秒，建议调整为30秒
2. **MaxUserPort**: 增加可用的用户端口范围，建议设置为65534（最大值）
3. **TCP自动调谐**: 启用TCP自动调谐功能，提高网络性能
4. **TcpFinWait2Delay**: 优化TCP连接释放速度 