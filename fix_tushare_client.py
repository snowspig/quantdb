# -*- coding: utf-8 -*-
# 修复tushare_client_wan.py文件中的缩进问题

def fix_indentation():
    # 文件路径
    file_path = 'core/tushare_client_wan.py'
    
    try:
        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # 修复第一个问题：is_port_available函数中的缩进
        for i in range(len(lines)):
            if "sock.settimeout(0.5)" in lines[i] and lines[i].strip() != "sock.settimeout(0.5)":
                indent = lines[i-1].split("sock")[0]  # 获取前一行的缩进
                lines[i] = indent + "sock.settimeout(0.5)\n"
                print(f"修复了第 {i+1} 行的缩进问题")
        
        # 修复第二个问题：with _port_cooldown_lock块的缩进
        for i in range(len(lines)):
            if "with _port_cooldown_lock:" in lines[i] and "self._release_port" in lines[i-1]:
                lines[i] = "                    with _port_cooldown_lock:\n"
                print(f"修复了第 {i+1} 行的缩进问题")
        
        # 将修改后的内容写回文件
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
        
        print("文件修复完成！")
        return True
    except Exception as e:
        print(f"修复过程中出错: {e}")
        return False

if __name__ == "__main__":
    fix_indentation()
