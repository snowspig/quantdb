#!/usr/bin/env python
"""
Windows Socket优化器 - 检测和优化Windows的TCP/IP设置

该模块用于检测Windows系统的TCP/IP设置并提供优化建议，
特别是对TIME_WAIT状态的持续时间进行优化，以减少端口复用时间。

注意：修改这些设置需要管理员权限，此脚本只提供检测和建议。
"""
import subprocess
import platform
import logging
from typing import Dict, Any, Tuple
import re

logger = logging.getLogger("core.wan_manager.win_socket_optimizer")

def is_windows() -> bool:
    """检查当前是否是Windows系统"""
    return platform.system().lower() == 'windows'

def is_admin() -> bool:
    """检查当前用户是否有管理员权限"""
    if not is_windows():
        return False
        
    try:
        # Windows下检查管理员权限
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except:
        return False

def run_command(cmd: str) -> Tuple[bool, str]:
    """运行命令并返回结果"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        return True, result.stdout
    except subprocess.TimeoutExpired:
        return False, "命令执行超时"
    except Exception as e:
        return False, f"执行命令失败: {str(e)}"

def get_tcp_parameters() -> Dict[str, Any]:
    """获取当前的TCP参数设置"""
    if not is_windows():
        return {'error': '不支持非Windows系统'}
        
    # 运行netsh命令获取TCP参数
    success, output = run_command('netsh int tcp show global')
    if not success:
        return {'error': output}
    
    params = {}
    
    # 解析输出
    for line in output.splitlines():
        line = line.strip()
        if not line or ':' not in line:
            continue
            
        key, value = line.split(':', 1)
        key = key.strip()
        value = value.strip()
        params[key] = value
    
    return params

def get_registry_tcp_params() -> Dict[str, Any]:
    """从注册表获取TCP参数"""
    if not is_windows():
        return {'error': '不支持非Windows系统'}
    
    params = {}
    
    # 运行reg命令查询TcpTimedWaitDelay值
    success, output = run_command(
        'reg query "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v "TcpTimedWaitDelay"'
    )
    
    if success and "TcpTimedWaitDelay" in output:
        # 解析REG_DWORD值
        match = re.search(r'REG_DWORD\s+0x([0-9a-fA-F]+)', output)
        if match:
            try:
                params['TcpTimedWaitDelay'] = int(match.group(1), 16)
            except:
                params['TcpTimedWaitDelay'] = 'Unknown'
    else:
        params['TcpTimedWaitDelay'] = 'Not set (default is 240 seconds)'
    
    # 查询其他有用的TCP参数
    tcp_params = [
        "MaxUserPort",             # 最大用户端口数
        "TcpMaxDataRetransmissions",  # TCP最大数据重传次数
        "MaxFreeTcbs",             # 最大空闲TCP控制块
        "MaxHashTableSize"         # 最大哈希表大小
    ]
    
    for param in tcp_params:
        success, output = run_command(
            f'reg query "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v "{param}"'
        )
        
        if success and param in output:
            match = re.search(r'REG_DWORD\s+0x([0-9a-fA-F]+)', output)
            if match:
                try:
                    params[param] = int(match.group(1), 16)
                except:
                    params[param] = 'Unknown'
        else:
            # 默认值
            if param == 'MaxUserPort':
                params[param] = 'Not set (default is 5000)'
            else:
                params[param] = 'Not set'
    
    return params

def get_tcp_statistics() -> Dict[str, Any]:
    """获取TCP连接统计信息"""
    if not is_windows():
        return {'error': '不支持非Windows系统'}
        
    # 运行netstat命令获取TCP统计
    success, output = run_command('netstat -s -p tcp')
    if not success:
        return {'error': output}
    
    stats = {}
    
    # 提取TCP统计数据
    time_wait_match = re.search(r'Time Waited Connections\s*=\s*(\d+)', output)
    if time_wait_match:
        stats['time_wait_connections'] = int(time_wait_match.group(1))
    
    active_match = re.search(r'Active Connections\s*=\s*(\d+)', output)
    if active_match:
        stats['active_connections'] = int(active_match.group(1))
    
    reset_match = re.search(r'Reset Connections\s*=\s*(\d+)', output)
    if reset_match:
        stats['reset_connections'] = int(reset_match.group(1))
        
    return stats

def generate_optimization_suggestions() -> Dict[str, Any]:
    """生成优化建议"""
    if not is_windows():
        return {
            'supported': False,
            'message': '不支持非Windows系统',
            'suggestions': []
        }
    
    # 获取当前设置
    tcp_params = get_tcp_parameters()
    registry_params = get_registry_tcp_params()
    
    suggestions = []
    commands = []
    
    # 检查TIME_WAIT延迟设置
    time_wait_delay = registry_params.get('TcpTimedWaitDelay')
    if isinstance(time_wait_delay, int):
        if time_wait_delay > 60:
            suggestions.append(f"TcpTimedWaitDelay值({time_wait_delay}秒)过高，建议设置为30秒")
            commands.append('reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v "TcpTimedWaitDelay" /t REG_DWORD /d 30 /f')
    else:
        suggestions.append("未设置TcpTimedWaitDelay，建议设置为30秒（默认为240秒）")
        commands.append('reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v "TcpTimedWaitDelay" /t REG_DWORD /d 30 /f')
    
    # 检查最大用户端口数
    max_user_port = registry_params.get('MaxUserPort')
    if not isinstance(max_user_port, int) or max_user_port < 32768:
        suggestions.append("MaxUserPort设置不足，建议设置为65534")
        commands.append('reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v "MaxUserPort" /t REG_DWORD /d 65534 /f')
    
    # 建议优化
    suggestions.append("启用TCP 1323选项以提高性能")
    commands.append('netsh int tcp set global rss=enabled chimney=enabled autotuninglevel=normal congestionprovider=ctcp')
    
    # 添加TIME_WAIT优化建议
    suggestions.append("优化TCP连接释放速度")
    commands.append('reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v "TcpFinWait2Delay" /t REG_DWORD /d 30 /f')
    commands.append('reg add "HKLM\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters" /v "KeepAliveTime" /t REG_DWORD /d 300000 /f')
    
    return {
        'supported': True,
        'is_admin': is_admin(),
        'message': '优化建议生成完成' if suggestions else '系统设置已经很好，无需优化',
        'current_settings': {
            'tcp_params': tcp_params,
            'registry_params': registry_params
        },
        'suggestions': suggestions,
        'commands': commands
    }

def apply_optimizations() -> Dict[str, Any]:
    """应用优化设置"""
    if not is_windows():
        return {
            'success': False,
            'message': '不支持非Windows系统'
        }
    
    if not is_admin():
        return {
            'success': False,
            'message': '需要管理员权限才能应用优化设置'
        }
    
    # 获取建议
    optimization = generate_optimization_suggestions()
    commands = optimization.get('commands', [])
    
    if not commands:
        return {
            'success': True,
            'message': '系统设置已经很好，无需优化'
        }
    
    # 执行命令
    results = []
    success_count = 0
    
    for cmd in commands:
        success, output = run_command(cmd)
        results.append({
            'command': cmd,
            'success': success,
            'output': output
        })
        if success:
            success_count += 1
    
    return {
        'success': success_count == len(commands),
        'message': f'执行了 {success_count}/{len(commands)} 条优化命令',
        'results': results
    }

def print_optimization_guide():
    """打印优化指南"""
    opt = generate_optimization_suggestions()
    
    if not opt['supported']:
        print(opt['message'])
        return
    
    print("\n===== Windows TCP/IP 优化指南 =====")
    print("\n当前设置:")
    
    # 打印当前注册表设置
    registry_params = opt['current_settings']['registry_params']
    print("\n注册表参数:")
    for key, value in registry_params.items():
        print(f"  {key}: {value}")
    
    # 打印优化建议
    print("\n优化建议:")
    for i, suggestion in enumerate(opt['suggestions'], 1):
        print(f"  {i}. {suggestion}")
    
    # 如果有建议，显示如何应用
    if opt['suggestions']:
        print("\n要应用这些优化，请在管理员权限的命令提示符中运行以下命令:")
        for i, cmd in enumerate(opt['commands'], 1):
            print(f"  {i}. {cmd}")
        
        print("\n注意: 应用这些更改后需要重启计算机才能生效")
    
    # 显示管理员权限状态
    print(f"\n当前状态: {'具有管理员权限' if opt['is_admin'] else '没有管理员权限 (需要管理员权限才能应用更改)'}")
    print("\n===================================")

def main():
    """主程序入口点，允许直接运行脚本进行TCP参数检查和优化"""
    print("Windows 网络优化工具")
    print("=" * 50)
    
    if not is_windows():
        print("该工具仅支持Windows系统。")
        return
    
    print("检查当前TCP参数配置...")
    print_optimization_guide()
    
    if is_admin():
        print("\n您当前具有管理员权限，可以直接应用优化。")
        
        user_input = input("是否应用推荐的优化设置? (y/n): ").strip().lower()
        if user_input == 'y':
            print("\n正在应用优化设置...")
            apply_optimizations()
            print("\n优化完成! 部分设置可能需要重启系统后生效。")
        else:
            print("\n您选择不应用优化设置。如需手动应用，请参照上述建议。")
    else:
        print("\n注意: 您需要管理员权限才能应用优化。")
        print("请右键点击命令提示符或PowerShell，选择'以管理员身份运行'，")
        print("然后重新运行此脚本: python -m core.wan_manager.win_socket_optimizer")

if __name__ == "__main__":
    main() 