package com.starrocks.backup.service;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;

/**
 * StarRocks 备份与恢复服务接口
 * 
 * 统一参数结构（Java 8 命名规范）：
 * {
 *   "methodName": "xxx",
 *   "params": {
 *     "backupClusterId": "ht",
 *     "restoreClusterId": "db",
 *     "databaseName": "lmp_label",
 *     "tableName": "table1",
 *     "byPartition": true,
 *     "partitionName": "p20260315"
 *   }
 * }
 */
public interface StarRocksBackupService {

    /**
     * 创建 HDFS 备份仓库
     * 参数: repoName, hdfsPath, brokerName, username, password
     */
    ObjectNode createRepository(Map<String, Object> params);

    /**
     * 显示所有仓库
     */
    ObjectNode showRepositories(Map<String, Object> params);

    /**
     * 显示数据库中的表
     * 参数: databaseName
     */
    ObjectNode showTables(Map<String, Object> params);

    /**
     * 备份表（支持分区级别）
     * 参数: databaseName, tableName, repoName, snapshotName, byPartition, partitionName
     */
    ObjectNode backupTable(Map<String, Object> params);

    /**
     * 查看备份进度
     * 参数: databaseName
     */
    ObjectNode showBackup(Map<String, Object> params);

    /**
     * 查看备份快照
     * 参数: repoName, snapshotName(可选)
     */
    ObjectNode showSnapshot(Map<String, Object> params);

    /**
     * 恢复快照
     * 参数: databaseName, snapshotName, repoName, tableName, newTableName, backupTimestamp, restoreClusterId
     */
    ObjectNode restoreSnapshot(Map<String, Object> params);

    /**
     * 查看恢复进度
     * 参数: databaseName
     */
    ObjectNode showRestore(Map<String, Object> params);

    /**
     * 整合的备份与恢复方法
     * 包含：备份表、查看备份进度、查看备份快照、恢复快照、查看恢复进度
     * 参数: databaseName, tableName, repoName, backupClusterId, restoreClusterId, byPartition, partitionName
     */
    ObjectNode backupAndRestore(Map<String, Object> params);
}
