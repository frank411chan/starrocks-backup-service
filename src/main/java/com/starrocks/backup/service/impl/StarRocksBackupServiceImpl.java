package com.starrocks.backup.service.impl;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.starrocks.backup.service.StarRocksBackupService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * StarRocks 备份与恢复服务实现
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StarRocksBackupServiceImpl implements StarRocksBackupService {

    private final JdbcTemplate jdbcTemplate;

    // ==================== 仓库管理 ====================

    @Override
    public JSONObject createRepository(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String repoName = (String) params.get("repoName");
        String location = (String) params.get("location");
        String broker = (String) params.getOrDefault("broker", "hdfs_broker");
        String username = (String) params.get("username");
        String password = (String) params.get("password");
        String properties = (String) params.getOrDefault("properties", "");

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE REPOSITORY `").append(repoName).append("` ");
            sql.append("WITH BROKER `").append(broker).append("` ");
            sql.append("ON LOCATION \"").append(location).append("\" ");
            sql.append("PROPERTIES (");
            sql.append("\"username\"=\"").append(username).append("\",");
            sql.append("\"password\"=\"").append(password).append("\"");
            if (!properties.isEmpty()) {
                sql.append(",").append(properties);
            }
            sql.append(")");

            log.info("执行SQL: {}", sql);
            jdbcTemplate.execute(sql.toString());

            result.put("code", 200);
            result.put("message", "仓库创建成功");
            result.put("repoName", repoName);
        } catch (Exception e) {
            log.error("创建仓库失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "创建仓库失败: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showRepositories(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        try {
            String sql = "SHOW REPOSITORIES";
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("data", list);
            result.put("total", list.size());
        } catch (Exception e) {
            log.error("查询仓库失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "查询失败: " + e.getMessage());
        }

        return result;
    }

    // ==================== 表管理 ====================

    @Override
    public JSONObject showTables(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = (String) params.get("database");
        if (database == null || database.isEmpty()) {
            database = "default_cluster";
        }

        try {
            String sql = "SHOW TABLES FROM " + database;
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("database", database);
            result.put("data", list);
            result.put("total", list.size());
        } catch (Exception e) {
            log.error("查询表失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "查询失败: " + e.getMessage());
        }

        return result;
    }

    // ==================== 备份管理 ====================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public JSONObject backupTable(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = (String) params.get("database");
        String tableName = (String) params.get("tableName");
        String repoName = (String) params.get("repoName");
        List<String> partitions = (List<String>) params.get("partitions");
        String snapshotName = (String) params.get("snapshotName");

        if (snapshotName == null || snapshotName.isEmpty()) {
            snapshotName = "snapshot_" + System.currentTimeMillis();
        }

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("BACKUP SNAPSHOT ").append(database).append(".").append(snapshotName);
            sql.append(" TO `").append(repoName).append("` ");
            sql.append("ON (").append(tableName);
            
            // 如果指定了分区，添加分区信息
            if (partitions != null && !partitions.isEmpty()) {
                sql.append(" PARTITION (");
                for (int i = 0; i < partitions.size(); i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(partitions.get(i));
                }
                sql.append(")");
            }
            
            sql.append(")");

            log.info("执行备份SQL: {}", sql);
            jdbcTemplate.execute(sql.toString());

            result.put("code", 200);
            result.put("message", "备份任务已提交");
            result.put("snapshotName", snapshotName);
            result.put("database", database);
            result.put("tableName", tableName);
            result.put("partitions", partitions);
        } catch (Exception e) {
            log.error("备份失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "备份失败: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showBackup(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = (String) params.get("database");
        if (database != null && !database.isEmpty()) {
            database = "FROM " + database;
        } else {
            database = "";
        }

        try {
            String sql = "SHOW BACKUP " + database;
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("data", list);
        } catch (Exception e) {
            log.error("查询备份进度失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "查询失败: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showSnapshot(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String repoName = (String) params.get("repoName");
        String snapshotName = (String) params.get("snapshotName");

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("SHOW SNAPSHOT ON `").append(repoName).append("`");
            if (snapshotName != null && !snapshotName.isEmpty()) {
                sql.append(" WHERE SNAPSHOT = \"").append(snapshotName).append("\"");
            }

            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql.toString());
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("data", list);
        } catch (Exception e) {
            log.error("查询快照失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "查询失败: " + e.getMessage());
        }

        return result;
    }

    // ==================== 恢复管理 ====================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public JSONObject restoreSnapshot(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = (String) params.get("database");
        String snapshotName = (String) params.get("snapshotName");
        String repoName = (String) params.get("repoName");
        String tableName = (String) params.get("tableName");
        String newTableName = (String) params.get("newTableName");
        String backupTimestamp = (String) params.get("backupTimestamp");
        String targetCluster = (String) params.get("targetCluster");

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("RESTORE SNAPSHOT ").append(database).append(".").append(snapshotName);
            sql.append(" FROM `").append(repoName).append("` ");
            sql.append("ON (").append(tableName);
            
            // 如果指定了新表名（用于恢复到不同集群）
            if (newTableName != null && !newTableName.isEmpty()) {
                sql.append(" AS ").append(newTableName);
            }
            
            sql.append(") ");
            sql.append("PROPERTIES (");
            sql.append("\"backup_timestamp\"=\"").append(backupTimestamp).append("\"");
            
            // 如果指定了目标集群（不同集群恢复）
            if (targetCluster != null && !targetCluster.isEmpty()) {
                sql.append(", \"cluster\"=\"").append(targetCluster).append("\"");
            }
            
            sql.append(")");

            log.info("执行恢复SQL: {}", sql);
            jdbcTemplate.execute(sql.toString());

            result.put("code", 200);
            result.put("message", "恢复任务已提交");
            result.put("snapshotName", snapshotName);
            result.put("database", database);
            result.put("tableName", tableName);
            result.put("newTableName", newTableName);
        } catch (Exception e) {
            log.error("恢复失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "恢复失败: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showRestore(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = (String) params.get("database");
        if (database != null && !database.isEmpty()) {
            database = "FROM " + database;
        } else {
            database = "";
        }

        try {
            String sql = "SHOW RESTORE " + database;
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("data", list);
        } catch (Exception e) {
            log.error("查询恢复进度失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "查询失败: " + e.getMessage());
        }

        return result;
    }

    // ==================== 整合方法 ====================

    @Override
    public JSONObject backupAndRestore(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        result.put("code", 200);
        result.put("message", "开始执行备份与恢复流程");
        
        JSONArray steps = new JSONArray();

        try {
            // 步骤1: 备份表
            log.info("步骤1: 备份表");
            JSONObject backupResult = backupTable(params);
            steps.add(createStepResult("backupTable", backupResult));
            
            if (backupResult.getIntValue("code") != 200) {
                result.put("code", 500);
                result.put("message", "备份失败，流程终止");
                result.put("steps", steps);
                return result;
            }

            String snapshotName = backupResult.getString("snapshotName");

            // 步骤2: 等待备份完成并查看进度
            log.info("步骤2: 查看备份进度");
            Thread.sleep(2000); // 等待2秒
            JSONObject showBackupResult = showBackup(params);
            steps.add(createStepResult("showBackup", showBackupResult));

            // 步骤3: 查看备份快照
            log.info("步骤3: 查看备份快照");
            params.put("snapshotName", snapshotName);
            JSONObject showSnapshotResult = showSnapshot(params);
            steps.add(createStepResult("showSnapshot", showSnapshotResult));

            // 步骤4: 恢复快照（如果指定了目标集群）
            String targetCluster = (String) params.get("targetCluster");
            if (targetCluster != null && !targetCluster.isEmpty()) {
                log.info("步骤4: 恢复快照到目标集群: {}", targetCluster);
                JSONObject restoreResult = restoreSnapshot(params);
                steps.add(createStepResult("restoreSnapshot", restoreResult));

                if (restoreResult.getIntValue("code") == 200) {
                    // 步骤5: 查看恢复进度
                    log.info("步骤5: 查看恢复进度");
                    Thread.sleep(2000);
                    JSONObject showRestoreResult = showRestore(params);
                    steps.add(createStepResult("showRestore", showRestoreResult));
                }
            }

            result.put("steps", steps);
            result.put("snapshotName", snapshotName);
            
        } catch (Exception e) {
            log.error("备份与恢复流程失败: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "流程执行失败: " + e.getMessage());
            result.put("steps", steps);
        }

        return result;
    }

    private JSONObject createStepResult(String stepName, JSONObject result) {
        JSONObject step = new JSONObject();
        step.put("step", stepName);
        step.put("code", result.getIntValue("code"));
        step.put("message", result.getString("message"));
        step.put("timestamp", System.currentTimeMillis());
        return step;
    }
}
