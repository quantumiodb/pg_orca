#!/bin/bash
# pg_orca 测试运行脚本

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"

# 打印带颜色的消息
info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 显示帮助
show_help() {
    cat << EOF
pg_orca 测试运行脚本

用法: $0 [选项] [测试名称]

选项:
  -h, --help              显示此帮助信息
  -b, --build             构建项目（如果尚未构建）
  -i, --install           安装扩展
  -c, --clean             清理构建目录
  -v, --verbose           详细输出
  -d, --diff              测试失败时显示差异
  --no-install-check      跳过扩展安装检查
  --update-expected       更新期望结果（慎用！）

测试名称（可选）:
  simple                  只运行 simple 测试（默认）
  base                    只运行 base 测试（需要 pg_tpch）
  tpch                    只运行 tpch 测试（需要 pg_tpch）
  tpcds                   只运行 tpcds 测试（需要 pg_tpcds）
  （不指定则运行所有 schedule 中的测试）

示例:
  $0                      # 运行所有测试（当前只有 simple）
  $0 -b -i                # 构建、安装并运行所有测试
  $0 simple               # 只运行 simple 测试
  $0 -v --diff            # 详细输出并在失败时显示差异

EOF
}

# 检查 PostgreSQL
check_postgres() {
    if ! command -v pg_config &> /dev/null; then
        error "未找到 pg_config，请安装 PostgreSQL 开发文件"
        exit 1
    fi

    info "PostgreSQL 版本: $(pg_config --version)"
    info "扩展目录: $(pg_config --pkglibdir)"
}

# 构建项目
build_project() {
    info "构建项目..."

    if [ ! -d "$BUILD_DIR" ]; then
        cmake -B "$BUILD_DIR" -G Ninja
    fi

    cmake --build "$BUILD_DIR"
    info "构建完成"
}

# 安装扩展
install_extension() {
    info "安装 pg_orca 扩展..."
    sudo cmake --build "$BUILD_DIR" --target install

    # 验证安装
    PGLIB=$(pg_config --pkglibdir)
    if [ -f "$PGLIB/pg_orca.so" ]; then
        info "扩展已成功安装到 $PGLIB/pg_orca.so"
    else
        error "扩展安装失败"
        exit 1
    fi
}

# 检查测试依赖
check_test_dependencies() {
    info "检查测试依赖..."
    info "当前测试套件不需要外部依赖"
    info "使用 simple.sql 测试文件（仅需 PostgreSQL 内置功能）"
}

# 清理构建目录
clean_build() {
    info "清理构建目录..."
    rm -rf "$BUILD_DIR"
    info "清理完成"
}

# 运行测试
run_tests() {
    local test_name="$1"
    local verbose="$2"

    if [ ! -d "$BUILD_DIR" ]; then
        error "构建目录不存在，请先构建项目 (-b 选项)"
        exit 1
    fi

    cd "$BUILD_DIR"

    if [ -n "$test_name" ]; then
        info "运行测试: $test_name"
    else
        info "运行所有测试..."
    fi

    if [ "$verbose" = "true" ]; then
        ctest --output-on-failure --verbose
    else
        ctest --output-on-failure
    fi
}

# 显示测试差异
show_diff() {
    info "查找测试差异文件..."

    if [ -d "$BUILD_DIR/test/results" ]; then
        local diff_files=$(find "$BUILD_DIR/test/results" -name "*.diff" 2>/dev/null)

        if [ -n "$diff_files" ]; then
            warn "发现测试差异:"
            for diff in $diff_files; do
                echo -e "${YELLOW}=== $(basename "$diff") ===${NC}"
                cat "$diff"
                echo ""
            done
        else
            info "未发现差异文件（所有测试通过或测试未运行）"
        fi
    else
        warn "测试结果目录不存在"
    fi
}

# 更新期望结果
update_expected() {
    warn "这将用当前测试结果覆盖期望输出文件"
    warn "确定要继续吗？(yes/no)"
    read -r response

    if [ "$response" = "yes" ]; then
        info "更新期望结果..."
        cmake --build "$BUILD_DIR" --target update_results
        info "期望结果已更新"
    else
        info "取消更新"
    fi
}

# 主函数
main() {
    local do_build=false
    local do_install=false
    local do_clean=false
    local verbose=false
    local show_diff_flag=false
    local skip_install_check=false
    local update_expected_flag=false
    local test_name=""

    # 解析参数
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -b|--build)
                do_build=true
                shift
                ;;
            -i|--install)
                do_install=true
                shift
                ;;
            -c|--clean)
                do_clean=true
                shift
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            -d|--diff)
                show_diff_flag=true
                shift
                ;;
            --no-install-check)
                skip_install_check=true
                shift
                ;;
            --update-expected)
                update_expected_flag=true
                shift
                ;;
            simple|base|tpch|tpcds)
                test_name="$1"
                shift
                ;;
            *)
                error "未知选项: $1"
                show_help
                exit 1
                ;;
        esac
    done

    info "pg_orca 测试运行器"
    echo ""

    check_postgres

    if [ "$do_clean" = true ]; then
        clean_build
        exit 0
    fi

    if [ "$do_build" = true ]; then
        build_project
    fi

    if [ "$do_install" = true ]; then
        install_extension
    fi

    if [ "$skip_install_check" = false ]; then
        check_test_dependencies
    fi

    if [ "$update_expected_flag" = true ]; then
        update_expected
        exit 0
    fi

    # 运行测试
    if run_tests "$test_name" "$verbose"; then
        info "✓ 测试完成"
        exit 0
    else
        error "✗ 测试失败"

        if [ "$show_diff_flag" = true ]; then
            echo ""
            show_diff
        else
            warn "使用 -d 或 --diff 选项查看详细差异"
        fi

        exit 1
    fi
}

main "$@"
