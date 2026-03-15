package com.starrocks.backup.service;

import com.alibaba.fastjson2.JSONObject;

import java.util.Map;

/**
 * StarRocks 备份与恢复服务接口
 * 
 * 统一参数结构：
 * {
 *   "方法名": "xxx",
 *   "参数": {
 *     "备份机房id": "ht",
 *     "恢复机房id": "db",
 *     "数据库名": "lmp_label",
 *     "表名": "table1",
 *     "byPartition": true,
 *     "分区名": "p20260315"
 *   }
 * }
 */
public interface StarRocksBackupService {

    /**
     * 创建 HDFS 备份仓库
     * 参数: 仓库名, HDFS路径, broker名, 用户名, 密码
     */
    JSONObject createRepository(Map<String, Object> params);

    /**
     * 显示所有仓库
     */
    JSONObject showRepositories(Map<String, Object> params);

    /**
     * 显示数据库中的表
     * 参数: 数据库名
     */
    JSONObject showTables(Map<String, Object> params);

    /**
     * 备份表（支持分区级别）
     * 参数: 数据库名, 表名, 仓库名, 快照名, byPartition, 分区名
     */
    JSONObject backupTable(Map<String, Object> params);

    /**
     * 查看备份进度
     * 参数: 数据库名
     */
    JSONObject showBackup(Map<String, Object> params);

    /**
     * 查看备份快照
     * 参数: 仓库名, 快照名(可选)
     */
    JSONObject showSnapshot(Map<String, Object> params);

    /**
     * 恢复快照
     * 参数: 数据库名, 快照名, 仓库名, 表名, 新表名, 备份时间戳, 恢复机房id
     */
    JSONObject restoreSnapshot(Map<String, Object> params);

    /**
     * 查看恢复进度
     * 参数: 数据库名
     */
    JSONObject showRestore(Map<String, Object> params);

    /**
     * 整合的备份与恢复方法
     * 包含：备份表、查看备份进度、查看备份快照、恢复快照、查看恢复进度
     * 参数: 数据库名, 表名, 仓库名, 备份机房id, 恢复机房id, byPartition, 分区名
     */
    JSONObject backupAndRestore(Map<String, Object> params);
}
