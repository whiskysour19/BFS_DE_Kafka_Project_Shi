## 项目目标

实现两个PostgreSQL数据库之间的实时数据同步系统，延迟小于1秒。

## 架构流程

```
源数据库(DB1) → CDC表 → Producer → Kafka → Consumer → 目标数据库(DB2)

```

## 核心组件

**1. 数据库设置**

- 两个独立的PostgreSQL数据库
- 源库：`employees` 表 + `emp_cdc` 表（CDC日志表）
- 目标库：`employees` 表

**2. CDC机制**

- 使用PostgreSQL触发器（Triggers）+ 函数（Functions）
- 自动捕获INSERT/UPDATE/DELETE操作
- 将变更记录写入`emp_cdc`表，包含action字段

**3. Producer**

- 扫描`emp_cdc`表
- 追踪已消费的offset（避免重复处理）
- 将变更记录发送到Kafka topic

**4. Consumer**

- 从Kafka消费变更记录
- 根据action类型（insert/update/delete）同步更新目标数据库

## 处理模式

- **快照处理**：初始全量数据同步
- **流式处理**：持续监听和同步增量变更

## 技术要点

- 使用PostgreSQL触发器方案（而非轮询）
- 需要offset管理机制防止数据重复
- 支持三种DML操作的实时同步
- 目标：实现两个PostgreSQL数据库之间的实时数据同步系统，延迟小于1秒。
- Deliverable
    
    roject 2的主要deliverables包括:
    
    1. **两个数据库配置** - 修改Docker Compose创建两个PostgreSQL数据库，映射到不同端口
    2. **数据库表结构**:
        - `employees` 表 (在两个数据库中)
        - `emp_cdc` 表 (CDC变更捕获表，包含action字段)
    3. **PostgreSQL触发器和函数** - 在employee_A表上设置触发器，将insert/update/delete操作记录到emp_cdc表
    4. **Producer** - 扫描emp_cdc表，将变更记录发送到Kafka topic
    5. **Consumer** - 从Kafka消费数据，根据action字段同步更新employee_B表
    
    **核心目标**: 实现两个数据库之间的实时数据同步（<1秒延迟），包括快照处理和流式处理（insert/update/delete操作）。