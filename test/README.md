# pg_orca 测试指南

## 测试结构

```
test/
├── sql/           # 测试 SQL 文件
│   ├── simple.sql # 简单测试（当前使用，无外部依赖）
│   ├── base.sql   # 基础测试（需要 pg_tpch，已禁用）
│   ├── tpch.sql   # TPC-H 基准测试（需要 pg_tpch，已禁用）
│   └── tpcds.sql  # TPC-DS 基准测试（需要 pg_tpcds，已禁用）
├── expected/      # 期望的输出结果
│   └── simple.out # simple 测试的期望输出
├── schedule       # 测试调度文件（当前只运行 simple）
└── regression.conf # PostgreSQL 配置
```

## 运行测试的方法

### 方法 1: 使用 CMake/CTest（推荐）

这是最简单的方法，会自动处理测试环境设置：

```bash
# 1. 构建项目
cmake -B build -G Ninja
cmake --build build

# 2. 安装扩展
sudo cmake --build build --target install

# 3. 运行所有测试
cd build
ctest --output-on-failure --verbose

# 或者使用详细输出的自定义目标
make test_verbose  # 如果使用 Unix Makefiles
# 或
ninja test_verbose # 如果使用 Ninja
```

CTest 会自动：
- 创建临时的 PostgreSQL 实例（在 `build/tmp_check` 目录）
- 加载 pg_orca 扩展
- 运行 schedule 文件中定义的测试
- 比较输出与期望结果

### 方法 2: 直接使用 pg_regress

如果你想手动控制测试过程：

```bash
# 确保 pg_orca 已安装
sudo cmake --build build --target install

# 查找 pg_regress 工具
PG_REGRESS=$(pg_config --pkglibdir)/pgxs/src/test/regress/pg_regress

# 运行测试
$PG_REGRESS \
  --temp-instance=./tmp_check \
  --temp-config=test/regression.conf \
  --inputdir=test \
  --outputdir=./test_results \
  --load-extension=pg_orca \
  --schedule test/schedule

# 或者运行单个测试
$PG_REGRESS \
  --temp-instance=./tmp_check \
  --temp-config=test/regression.conf \
  --inputdir=test \
  --outputdir=./test_results \
  --load-extension=pg_orca \
  simple  # 只运行 simple 测试
```

### 方法 3: 在现有数据库中手动测试

如果你想在现有的 PostgreSQL 实例中测试：

```bash
# 1. 启动 PostgreSQL 并连接
psql -d postgres

# 2. 在 psql 中加载扩展
postgres=# LOAD 'pg_orca';
-- 或者配置 shared_preload_libraries = 'pg_orca' 后重启 PostgreSQL

# 3. 手动运行测试 SQL
postgres=# \i test/sql/simple.sql

# 4. 比较输出
# 可以将输出重定向到文件并与 expected/simple.out 比较
```

## 测试说明

### simple.sql（当前使用）
- **依赖**: 无 - 仅使用 PostgreSQL 内置功能
- 测试 ORCA 优化器的核心功能：
  - 基本的 SELECT、INSERT 操作
  - WHERE 子句和表达式
  - JOIN 操作（INNER、LEFT）
  - 聚合函数和 GROUP BY
  - 子查询和 CTE
  - UNION、EXCEPT、INTERSECT
  - 各种数据类型和类型转换

### 旧测试文件（已禁用）

以下测试文件需要外部扩展，当前已从 schedule 中移除：

- **base.sql**: 需要 pg_tpch 扩展
- **tpch.sql**: 需要 pg_tpch 扩展（TPC-H 基准测试）
- **tpcds.sql**: 需要 pg_tpcds 扩展（TPC-DS 基准测试）

如果需要运行这些测试，请手动安装相应的扩展并修改 `test/schedule` 文件。

## 测试输出

测试结果会保存在：
- **CTest**: `build/test/results/*.out`
- **pg_regress**: 你指定的 `--outputdir` 目录

如果测试失败，会生成差异文件 (`.diff`)，显示期望输出和实际输出的差异。

## 更新期望结果

如果你修改了 ORCA 的行为并且新的输出是正确的：

```bash
# 使用 CMake 提供的目标
cd build
cmake --build . --target update_results

# 或者手动复制
cp build/test/results/*.out test/expected/
```

## 调试测试失败

```bash
# 1. 运行详细输出的测试
cd build
ctest --output-on-failure --verbose

# 2. 查看差异文件
cat build/test/results/*.diff

# 3. 检查临时实例的日志
cat build/tmp_check/log/*.log

# 4. 保留临时实例进行调试
# 修改 pg_regress 命令，移除 --temp-instance 清理
```

## 只运行部分测试

修改 `test/schedule` 文件：

```bash
# 只运行 simple 测试（当前默认）
test: simple

# 如果安装了依赖，可以运行多个测试
test: simple base tpch

# 并行运行测试（如果支持）
test: simple
test: base tpch
```

## 示例：最小化测试工作流

```bash
# 快速测试流程
cmake -B build -G Ninja
cmake --build build
sudo cmake --build build --target install
cd build
ctest --output-on-failure

# 如果失败，查看差异
cat test/results/*.diff
```

## 注意事项

1. **扩展加载**: 确保 PostgreSQL 能找到 pg_orca.so（通过 `pg_config --pkglibdir` 检查）
2. **权限**: 安装扩展需要 sudo 权限
3. **PostgreSQL 版本**: pg_orca 基于 PostgreSQL 17，其他版本未测试
4. **临时实例**: pg_regress 会创建临时 PostgreSQL 实例，测试完成后自动清理
5. **端口冲突**: 如果默认端口被占用，pg_regress 会自动选择其他端口

## 故障排除

### 错误: "could not load library pg_orca.so"
```bash
# 检查扩展是否已安装
ls $(pg_config --pkglibdir)/pg_orca.so

# 重新安装
sudo cmake --build build --target install
```

### 运行旧的 TPC-H/TPC-DS 测试
```bash
# 如果需要运行需要外部依赖的旧测试：
# 1. 安装 pg_tpch 或 pg_tpcds 扩展
# 2. 编辑 test/schedule，添加相应的测试名称
#    例如: test: simple base tpch
```

### 测试挂起或超时
```bash
# 检查 PostgreSQL 进程
ps aux | grep postgres

# 清理残留的测试实例
pkill -f tmp_check
rm -rf build/tmp_check
```
