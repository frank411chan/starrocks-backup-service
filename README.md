# StarRocks 备份与恢复微服务

基于 Spring Boot 2.x + MyBatis 的 StarRocks 数据库备份与恢复微服务。

## 功能特性

- ✅ **仓库管理**：创建 HDFS 备份仓库、查看仓库列表
- ✅ **表管理**：查看数据库中的表
- ✅ **备份管理**：支持表级别和分区级别备份
- ✅ **恢复管理**：支持恢复到原集群或不同集群（跨机房）
- ✅ **进度查询**：查看备份/恢复进度和快照
- ✅ **整合流程**：一键完成备份+恢复全流程

## 技术栈

- Spring Boot 2.7.18
- MyBatis + MySQL Connector
- Druid 连接池
- FastJSON2
- Hutool 工具包

## 快速开始

### 1. 配置数据库连接

编辑 `src/main/resources/application.yml`：

```yaml
spring:
  datasource:
    source:
      # 源集群（备份来源）
      url: jdbc:mysql://ht-starrocks-host:9030/lmp_label?useSSL=false
      username: root
      password: your_password
    
    target:
      # 目标集群（恢复目标，可选）
      url: jdbc:mysql://db-starrocks-host:9030/lmp_label?useSSL=false
      username: root
      password: your_password
```

### 2. 编译运行

```bash
mvn clean package
java -jar target/starrocks-backup-service-1.0.0.jar
```

### 3. 测试接口

```bash
curl -X POST http://localhost:8080/api/starrocks/execute \
  -H "Content-Type: application/json" \
  -d '{
    "方法名": "显示表",
    "参数": {
      "数据库名": "lmp_label"
    }
  }'
```

## API 接口

### 统一入口

```
POST /api/starrocks/execute
Content-Type: application/json
```

### 请求参数结构

```json
{
  "方法名": "方法名称",
  "参数": {
    "备份机房id": "ht",
    "恢复机房id": "db",
    "数据库名": "lmp_label",
    "表名": "table1",
    "byPartition": true,
    "分区名": "p20260315"
  }
}
```

### 支持的方法

#### 仓库管理
- `创建HDFS仓库` - 创建 HDFS 备份仓库
- `显示仓库列表` - 查看所有仓库

#### 表管理
- `显示表` - 查看数据库中的表

#### 备份管理
- `备份表` - 备份表（支持分区）
- `查看备份进度` - 查看备份进度
- `查看备份快照` - 查看备份快照

#### 恢复管理
- `恢复快照` - 恢复快照
- `查看恢复进度` - 查看恢复进度

#### 整合方法
- `备份与恢复表` - 备份+恢复全流程

## 使用示例

### 1. 创建备份仓库

```json
{
  "方法名": "创建HDFS仓库",
  "参数": {
    "仓库名": "hdfs_repo",
    "HDFS路径": "hdfs://namenode:9000/starrocks/backup",
    "broker名": "hdfs_broker",
    "用户名": "hdfs_user",
    "密码": "hdfs_pass"
  }
}
```

### 2. 备份表（全表）

```json
{
  "方法名": "备份表",
  "参数": {
    "数据库名": "lmp_label",
    "表名": "table1",
    "仓库名": "hdfs_repo",
    "快照名": "snapshot_table1_20240315",
    "byPartition": false
  }
}
```

### 3. 备份表（指定分区）

```json
{
  "方法名": "备份表",
  "参数": {
    "数据库名": "lmp_label",
    "表名": "table1",
    "仓库名": "hdfs_repo",
    "快照名": "snapshot_table1_p20260315",
    "byPartition": true,
    "分区名": "p20260315"
  }
}
```

### 4. 备份多个分区

```json
{
  "方法名": "备份表",
  "参数": {
    "数据库名": "lmp_label",
    "表名": "table1",
    "仓库名": "hdfs_repo",
    "byPartition": true,
    "分区名": ["p20260315", "p20260316", "p20260317"]
  }
}
```

### 5. 恢复到不同机房

```json
{
  "方法名": "恢复快照",
  "参数": {
    "数据库名": "lmp_label",
    "快照名": "snapshot_table1_20240315",
    "仓库名": "hdfs_repo",
    "表名": "table1",
    "新表名": "table1_backup",
    "备份时间戳": "2024-03-15-10-30-00",
    "恢复机房id": "db"
  }
}
```

### 6. 整合备份与恢复（跨机房）

```json
{
  "方法名": "备份与恢复表",
  "参数": {
    "备份机房id": "ht",
    "恢复机房id": "db",
    "数据库名": "lmp_label",
    "表名": "table1",
    "仓库名": "hdfs_repo",
    "byPartition": true,
    "分区名": "p20260315"
  }
}
```

## 参数说明

| 参数名 | 类型 | 必填 | 说明 |
|-------|------|:---:|------|
| 方法名 | String | ✅ | 要执行的方法 |
| 参数 | Object | ✅ | 方法参数 |
| 备份机房id | String | 条件 | 源集群标识，如 "ht" |
| 恢复机房id | String | 条件 | 目标集群标识，如 "db" |
| 数据库名 | String | ✅ | 数据库名称 |
| 表名 | String | ✅ | 表名称 |
| byPartition | Boolean | 可选 | 是否按分区备份，默认 false |
| 分区名 | String/List | 条件 | 分区名称，支持单分区或多分区 |
| 仓库名 | String | ✅ | 备份仓库名称 |
| 快照名 | String | 可选 | 快照名称，默认自动生成 |
| 新表名 | String | 可选 | 恢复后的新表名 |
| 备份时间戳 | String | 可选 | 备份时间戳 |

## StarRocks 版本要求

- StarRocks 2.5.13+
- 支持 HDFS 或 S3 作为备份仓库

## 许可证

MIT License
