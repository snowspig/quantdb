#!/bin/bash
# push_to_github.sh - 将代码推送到GitHub仓库的便捷脚本

# 设置变量
REPO_URL="https://github.com/snowspig/quantdb.git"
BRANCH="main"
COMMIT_MSG="Update: gorepe.py任务调度系统及相关组件实现"

# 输出彩色文本函数
echo_color() {
    local color=$1
    local text=$2
    case $color in
        "red") echo -e "\033[0;31m$text\033[0m" ;;
        "green") echo -e "\033[0;32m$text\033[0m" ;;
        "yellow") echo -e "\033[0;33m$text\033[0m" ;;
        "blue") echo -e "\033[0;34m$text\033[0m" ;;
        *) echo "$text" ;;
    esac
}

# 显示帮助信息
show_help() {
    echo "使用方法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -m, --message   指定提交信息 (默认: '$COMMIT_MSG')"
    echo "  -b, --branch    指定分支名称 (默认: '$BRANCH')"
    echo "  -r, --repo      指定仓库URL (默认: '$REPO_URL')"
    echo "  -h, --help      显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 -m '实现新功能' -b develop"
}

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -m|--message)
            COMMIT_MSG="$2"
            shift
            shift
            ;;
        -b|--branch)
            BRANCH="$2"
            shift
            shift
            ;;
        -r|--repo)
            REPO_URL="$2"
            shift
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "未知选项: $1"
            show_help
            exit 1
            ;;
    esac
done

# 显示配置信息
echo_color "blue" "==== GitHub 推送配置 ===="
echo "仓库URL: $REPO_URL"
echo "分支: $BRANCH"
echo "提交信息: $COMMIT_MSG"
echo "========================="

# 检查 git 是否已安装
if ! command -v git &> /dev/null; then
    echo_color "red" "错误: git 未安装，请先安装 git"
    exit 1
fi

# 检查当前目录是否是 git 仓库
if [ ! -d ".git" ]; then
    echo_color "yellow" "当前目录不是 git 仓库，正在初始化..."
    git init
    
    # 添加远程仓库
    echo_color "blue" "添加远程仓库: $REPO_URL"
    git remote add origin $REPO_URL
else
    # 检查远程仓库是否已配置
    if ! git remote -v | grep -q origin; then
        echo_color "blue" "添加远程仓库: $REPO_URL"
        git remote add origin $REPO_URL
    elif [ "$(git remote get-url origin)" != "$REPO_URL" ]; then
        echo_color "yellow" "更新远程仓库URL: $REPO_URL"
        git remote set-url origin $REPO_URL
    fi
fi

# 添加文件到暂存区
echo_color "blue" "添加文件到暂存区..."
git add .

# 提交更改
echo_color "blue" "提交更改: $COMMIT_MSG"
git commit -m "$COMMIT_MSG"

# 拉取最新代码(如果可能)
echo_color "blue" "拉取远程分支 $BRANCH 的最新代码..."
git pull origin $BRANCH --no-rebase || echo_color "yellow" "无法拉取远程分支，可能是新分支或网络问题"

# 推送代码
echo_color "blue" "推送代码到远程分支 $BRANCH..."
if git push origin $BRANCH; then
    echo_color "green" "成功: 代码已推送到 $REPO_URL 的 $BRANCH 分支"
else
    echo_color "red" "错误: 推送失败，请检查您的权限和网络连接"
    echo "提示: 如果是首次推送，请尝试: git push -u origin $BRANCH"
    echo "如果需要身份认证，请配置 git 凭据"
    exit 1
fi

echo_color "green" "完成!"
