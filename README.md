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
- JUnit 5 + Mockito（单元测试）

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

### 3. 运行单元测试

```bash
mvn test
```

## API 接口

### 统一入口

```
POST /api/starrocks/execute
Content-Type: application/json
```

### 请求参数结构（Java 8 命名规范）

```json
{
  "methodName": "backupAndRestore",
  "params": {
    "backupClusterId": "ht",
    "restoreClusterId": "db",
    "databaseName": "lmp_label",
    "tableName": "table1",
    "byPartition": true,
    "partitionName": "p20260315"
  }
}
```

### 参数说明

| 参数名 | 类型 | 必填 | 说明 |
|-------|------|:---:|------|
| methodName | String | ✅ | 方法名（驼峰命名） |
| params | Object | ✅ | 方法参数 |
| backupClusterId | String | 条件 | 源集群标识，如 "ht" |
| restoreClusterId | String | 条件 | 目标集群标识，如 "db" |
| databaseName | String | ✅ | 数据库名称 |
| tableName | String | ✅ | 表名称 |
| byPartition | Boolean | 可选 | 是否按分区备份，默认 false |
| partitionName | String/List | 条件 | 分区名称，支持单分区或多分区 |
| repoName | String | ✅ | 备份仓库名称 |
| snapshotName | String | 可选 | 快照名称，默认自动生成 |
| newTableName | String | 可选 | 恢复后的新表名 |
| backupTimestamp | String | 可选 | 备份时间戳 |

### 支持的方法

#### 仓库管理
- `createRepository` - 创建 HDFS 备份仓库
- `showRepositories` - 查看所有仓库

#### 表管理
- `showTables` - 查看数据库中的表

#### 备份管理
- `backupTable` - 备份表（支持分区）
- `showBackup` - 查看备份进度
- `showSnapshot` - 查看备份快照

#### 恢复管理
- `restoreSnapshot` - 恢复快照
- `showRestore` - 查看恢复进度

#### 整合方法
- `backupAndRestore` - 备份+恢复全流程

## 使用示例

### 1. 创建备份仓库

```json
{
  "methodName": "createRepository",
  "params": {
    "repoName": "hdfs_repo",
    "hdfsPath": "hdfs://namenode:9000/starrocks/backup",
    "brokerName": "hdfs_broker",
    "username": "hdfs_user",
    "password": "hdfs_pass"
  }
}
```

### 2. 备份表（全表）

```json
{
  "methodName": "backupTable",
  "params": {
    "databaseName": "lmp_label",
    "tableName": "table1",
    "repoName": "hdfs_repo",
    "snapshotName": "snapshot_table1_20240315",
    "byPartition": false
  }
}
```

### 3. 备份表（指定分区）

```json
{
  "methodName": "backupTable",
  "params": {
    "databaseName": "lmp_label",
    "tableName": "table1",
    "repoName": "hdfs_repo",
    "snapshotName": "snapshot_table1_p20260315",
    "byPartition": true,
    "partitionName": "p20260315"
  }
}
```

### 4. 备份多个分区

```json
{
  "methodName": "backupTable",
  "params": {
    "databaseName": "lmp_label",
    "tableName": "table1",
    "repoName": "hdfs_repo",
    "byPartition": true,
    "partitionName": ["p20260315", "p20260316", "p20260317"]
  }
}
```

### 5. 恢复到不同机房

```json
{
  "methodName": "restoreSnapshot",
  "params": {
    "databaseName": "lmp_label",
    "snapshotName": "snapshot_table1_20240315",
    "repoName": "hdfs_repo",
    "tableName": "table1",
    "newTableName": "table1_backup",
    "backupTimestamp": "2024-03-15-10-30-00",
    "restoreClusterId": "db"
  }
}
```

### 6. 整合备份与恢复（跨机房）

```json
{
  "methodName": "backupAndRestore",
  "params": {
    "backupClusterId": "ht",
    "restoreClusterId": "db",
    "databaseName": "lmp_label",
    "tableName": "table1",
    "repoName": "hdfs_repo",
    "byPartition": true,
    "partitionName": "p20260315"
  }
}
```

## 单元测试

### 测试覆盖

- ✅ Controller 层测试
- ✅ Service 层测试
- ✅ 参数解析测试
- ✅ 异常处理测试

### 运行测试

```bash
# 运行所有测试
mvn test

# 运行指定测试类
mvn test -Dtest=StarRocksBackupServiceImplTest

# 生成测试报告
mvn surefire-report:report
```

### 测试报告位置

```
target/surefire-reports/
```

## StarRocks 版本要求

- StarRocks 2.5.13+
- 支持 HDFS 或 S3 作为备份仓库

## 许可证

MIT License
