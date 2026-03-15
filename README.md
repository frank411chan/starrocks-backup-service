# StarRocks 备份与恢复微服务

基于 Spring Boot 2.x + MyBatis 的 StarRocks 数据库备份与恢复微服务。

## 功能特性

- ✅ **仓库管理**：创建 HDFS 备份仓库、查看仓库列表
- ✅ **表管理**：查看数据库中的表
- ✅ **备份管理**：支持表级别和分区级别备份
- ✅ **恢复管理**：支持恢复到原集群或不同集群
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
      url: jdbc:mysql://source-host:9030/default_cluster
      username: root
      password: your_password
    
    target:
      # 目标集群（恢复目标，可选）
      url: jdbc:mysql://target-host:9030/default_cluster
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
    "method": "showTables",
    "database": "default_cluster"
  }'
```

## API 接口

### 统一入口

```
POST /api/starrocks/execute
Content-Type: application/json
```

### 请求参数

| 参数 | 类型 | 必填 | 说明 |
|-----|------|:---:|------|
| method | String | ✅ | 方法名 |
| ... | - | - | 其他参数 |

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
  "method": "createRepository",
  "repoName": "hdfs_repo",
  "location": "hdfs://namenode:9000/starrocks/backup",
  "broker": "hdfs_broker",
  "username": "hdfs_user",
  "password": "hdfs_pass"
}
```

### 2. 备份表

```json
{
  "method": "backupTable",
  "database": "db1",
  "tableName": "table1",
  "repoName": "hdfs_repo",
  "snapshotName": "snapshot_20240315"
}
```

### 3. 备份指定分区

```json
{
  "method": "backupTable",
  "database": "db1",
  "tableName": "table1",
  "repoName": "hdfs_repo",
  "snapshotName": "snapshot_20240315",
  "partitions": ["p1", "p2", "p3"]
}
```

### 4. 恢复到不同集群

```json
{
  "method": "restoreSnapshot",
  "database": "db1",
  "snapshotName": "snapshot_20240315",
  "repoName": "hdfs_repo",
  "tableName": "table1",
  "newTableName": "table1_backup",
  "backupTimestamp": "2024-03-15-10-30-00",
  "targetCluster": "cluster2"
}
```

### 5. 整合备份与恢复

```json
{
  "method": "backupAndRestore",
  "database": "db1",
  "tableName": "table1",
  "repoName": "hdfs_repo",
  "snapshotName": "snapshot_20240315",
  "targetCluster": "cluster2",
  "newTableName": "table1_backup"
}
```

## StarRocks 版本要求

- StarRocks 2.5.13+
- 支持 HDFS 或 S3 作为备份仓库

## 许可证

MIT License
