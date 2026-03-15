package com.starrocks.backup.service;

import com.alibaba.fastjson2.JSONObject;

import java.util.Map;

/**
 * StarRocks 备份与恢复服务接口
 */
public interface StarRocksBackupService {

    /**
     * 创建 HDFS 备份仓库
     */
    JSONObject createRepository(Map<String, Object> params);

    /**
     * 显示所有仓库
     */
    JSONObject showRepositories(Map<String, Object> params);

    /**
     * 显示数据库中的表
     */
    JSONObject showTables(Map<String, Object> params);

    /**
     * 备份表（支持分区级别）
     */
    JSONObject backupTable(Map<String, Object> params);

    /**
     * 查看备份进度
     */
    JSONObject showBackup(Map<String, Object> params);

    /**
     * 查看备份快照
     */
    JSONObject showSnapshot(Map<String, Object> params);

    /**
     * 恢复快照
     */
    JSONObject restoreSnapshot(Map<String, Object> params);

    /**
     * 查看恢复进度
     */
    JSONObject showRestore(Map<String, Object> params);

    /**
     * 整合的备份与恢复方法
     * 包含：备份表、查看备份进度、查看备份快照、恢复快照、查看恢复进度
     */
    JSONObject backupAndRestore(Map<String, Object> params);
}
