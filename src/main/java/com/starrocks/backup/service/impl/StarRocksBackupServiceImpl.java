package com.starrocks.backup.service.impl;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.starrocks.backup.service.StarRocksBackupService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * StarRocks 备份与恢复服务实现
 * 
 * 参数说明（Java 8 命名规范）：
 * - backupClusterId: 源集群标识，如 "ht"
 * - restoreClusterId: 目标集群标识，如 "db"
 * - databaseName: 数据库名称，如 "lmp_label"
 * - tableName: 表名称，如 "table1"
 * - byPartition: 是否按分区备份，true/false
 * - partitionName: 分区名称，支持 String 或 List<String>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StarRocksBackupServiceImpl implements StarRocksBackupService {

    private final JdbcTemplate jdbcTemplate;

    // ==================== 参数解析工具方法 ====================

    private String getStringParam(Map<String, Object> params, String key, String defaultValue) {
        Object value = params.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private String getStringParam(Map<String, Object> params, String key) {
        return getStringParam(params, key, null);
    }

    @SuppressWarnings("unchecked")
    private List<String> getPartitionNames(Map<String, Object> params) {
        List<String> partitions = new ArrayList<>();
        
        Object partitionObj = params.get("partitionName");
        if (partitionObj instanceof List) {
            partitions = (List<String>) partitionObj;
        } else if (partitionObj instanceof String) {
            String partitionStr = (String) partitionObj;
            if (!partitionStr.isEmpty()) {
                for (String p : partitionStr.split(",")) {
                    partitions.add(p.trim());
                }
            }
        }
        
        return partitions;
    }

    private boolean isByPartition(Map<String, Object> params) {
        Object byPartition = params.get("byPartition");
        if (byPartition instanceof Boolean) {
            return (Boolean) byPartition;
        }
        return Boolean.parseBoolean(String.valueOf(byPartition));
    }

    // ==================== 仓库管理 ====================

    @Override
    public JSONObject createRepository(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String repoName = getStringParam(params, "repoName");
        String hdfsPath = getStringParam(params, "hdfsPath");
        String brokerName = getStringParam(params, "brokerName", "hdfs_broker");
        String username = getStringParam(params, "username");
        String password = getStringParam(params, "password");

        if (repoName == null || hdfsPath == null) {
            return errorResponse("repoName and hdfsPath cannot be empty");
        }

        try {
            String sql = String.format(
                "CREATE REPOSITORY `%s` WITH BROKER `%s` ON LOCATION \"%s\" PROPERTIES (\"username\"=\"%s\",\"password\"=\"%s\")",
                repoName, brokerName, hdfsPath, username, password
            );

            log.info("Execute SQL: {}", sql);
            jdbcTemplate.execute(sql);

            result.put("code", 200);
            result.put("message", "Repository created successfully");
            result.put("repoName", repoName);
        } catch (Exception e) {
            log.error("Create repository failed: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "Create repository failed: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showRepositories(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList("SHOW REPOSITORIES");
            
            result.put("code", 200);
            result.put("message", "Query successful");
            result.put("data", list);
            result.put("total", list.size());
        } catch (Exception e) {
            log.error("Query repositories failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }

        return result;
    }

    // ==================== 表管理 ====================

    @Override
    public JSONObject showTables(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String databaseName = getStringParam(params, "databaseName", "default_cluster");

        try {
            String sql = "SHOW TABLES FROM " + databaseName;
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "Query successful");
            result.put("databaseName", databaseName);
            result.put("data", list);
            result.put("total", list.size());
        } catch (Exception e) {
            log.error("Query tables failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }

        return result;
    }

    // ==================== 备份管理 ====================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public JSONObject backupTable(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String databaseName = getStringParam(params, "databaseName");
        String tableName = getStringParam(params, "tableName");
        String repoName = getStringParam(params, "repoName");
        String snapshotName = getStringParam(params, "snapshotName");
        
        boolean byPartition = isByPartition(params);
        List<String> partitionNames = getPartitionNames(params);

        if (databaseName == null || tableName == null || repoName == null) {
            return errorResponse("databaseName, tableName, repoName cannot be empty");
        }

        if (snapshotName == null || snapshotName.isEmpty()) {
            snapshotName = String.format("%s_%s_%d", databaseName, tableName, System.currentTimeMillis());
        }

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("BACKUP SNAPSHOT ").append(databaseName).append(".").append(snapshotName);
            sql.append(" TO `").append(repoName).append("` ");
            sql.append("ON (").append(tableName);
            
            if (byPartition && !partitionNames.isEmpty()) {
                sql.append(" PARTITION (");
                for (int i = 0; i < partitionNames.size(); i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(partitionNames.get(i));
                }
                sql.append(")");
            }
            
            sql.append(")");

            log.info("Execute backup SQL: {}", sql);
            jdbcTemplate.execute(sql.toString());

            result.put("code", 200);
            result.put("message", "Backup task submitted");
            result.put("snapshotName", snapshotName);
            result.put("databaseName", databaseName);
            result.put("tableName", tableName);
            result.put("byPartition", byPartition);
            result.put("partitionName", partitionNames);
        } catch (Exception e) {
            log.error("Backup failed: {}", e.getMessage(), e);
            return errorResponse("Backup failed: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showBackup(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String databaseName = getStringParam(params, "databaseName");
        String sql = (databaseName != null && !databaseName.isEmpty()) 
            ? "SHOW BACKUP FROM " + databaseName 
            : "SHOW BACKUP";

        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "Query successful");
            result.put("data", list);
        } catch (Exception e) {
            log.error("Query backup progress failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showSnapshot(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String repoName = getStringParam(params, "repoName");
        String snapshotName = getStringParam(params, "snapshotName");

        if (repoName == null) {
            return errorResponse("repoName cannot be empty");
        }

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("SHOW SNAPSHOT ON `").append(repoName).append("`");
            if (snapshotName != null && !snapshotName.isEmpty()) {
                sql.append(" WHERE SNAPSHOT = \"").append(snapshotName).append("\"");
            }

            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql.toString());
            
            result.put("code", 200);
            result.put("message", "Query successful");
            result.put("data", list);
        } catch (Exception e) {
            log.error("Query snapshot failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }

        return result;
    }

    // ==================== 恢复管理 ====================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public JSONObject restoreSnapshot(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String databaseName = getStringParam(params, "databaseName");
        String snapshotName = getStringParam(params, "snapshotName");
        String repoName = getStringParam(params, "repoName");
        String tableName = getStringParam(params, "tableName");
        String newTableName = getStringParam(params, "newTableName");
        String backupTimestamp = getStringParam(params, "backupTimestamp");
        String restoreClusterId = getStringParam(params, "restoreClusterId");

        if (databaseName == null || snapshotName == null || repoName == null || tableName == null) {
            return errorResponse("databaseName, snapshotName, repoName, tableName cannot be empty");
        }

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("RESTORE SNAPSHOT ").append(databaseName).append(".").append(snapshotName);
            sql.append(" FROM `").append(repoName).append("` ");
            sql.append("ON (").append(tableName);
            
            if (newTableName != null && !newTableName.isEmpty()) {
                sql.append(" AS ").append(newTableName);
            }
            
            sql.append(") ");
            sql.append("PROPERTIES (");
            
            if (backupTimestamp != null && !backupTimestamp.isEmpty()) {
                sql.append("\"backup_timestamp\"=\"").append(backupTimestamp).append("\"");
            }
            
            if (restoreClusterId != null && !restoreClusterId.isEmpty()) {
                if (backupTimestamp != null) sql.append(", ");
                sql.append("\"cluster\"=\"").append(restoreClusterId).append("\"");
            }
            
            sql.append(")");

            log.info("Execute restore SQL: {}", sql);
            jdbcTemplate.execute(sql.toString());

            result.put("code", 200);
            result.put("message", "Restore task submitted");
            result.put("snapshotName", snapshotName);
            result.put("databaseName", databaseName);
            result.put("tableName", tableName);
            result.put("newTableName", newTableName);
            result.put("restoreClusterId", restoreClusterId);
        } catch (Exception e) {
            log.error("Restore failed: {}", e.getMessage(), e);
            return errorResponse("Restore failed: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showRestore(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String databaseName = getStringParam(params, "databaseName");
        String sql = (databaseName != null && !databaseName.isEmpty()) 
            ? "SHOW RESTORE FROM " + databaseName 
            : "SHOW RESTORE";

        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "Query successful");
            result.put("data", list);
        } catch (Exception e) {
            log.error("Query restore progress failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }

        return result;
    }

    // ==================== 整合方法 ====================

    @Override
    public JSONObject backupAndRestore(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        result.put("code", 200);
        result.put("message", "Starting backup and restore process");
        
        JSONArray steps = new JSONArray();

        try {
            // Step 1: Backup table
            log.info("Step 1: Backup table");
            JSONObject backupResult = backupTable(params);
            steps.add(createStepResult("backupTable", backupResult));
            
            if (backupResult.getIntValue("code") != 200) {
                result.put("code", 500);
                result.put("message", "Backup failed, process terminated");
                result.put("steps", steps);
                return result;
            }

            String snapshotName = backupResult.getString("snapshotName");

            // Step 2: Show backup progress
            log.info("Step 2: Show backup progress");
            Thread.sleep(2000);
            JSONObject showBackupResult = showBackup(params);
            steps.add(createStepResult("showBackup", showBackupResult));

            // Step 3: Show snapshot
            log.info("Step 3: Show snapshot");
            params.put("snapshotName", snapshotName);
            JSONObject showSnapshotResult = showSnapshot(params);
            steps.add(createStepResult("showSnapshot", showSnapshotResult));

            // Step 4: Restore snapshot (if target cluster specified)
            String restoreClusterId = getStringParam(params, "restoreClusterId");
            if (restoreClusterId != null && !restoreClusterId.isEmpty()) {
                log.info("Step 4: Restore snapshot to cluster: {}", restoreClusterId);
                JSONObject restoreResult = restoreSnapshot(params);
                steps.add(createStepResult("restoreSnapshot", restoreResult));

                if (restoreResult.getIntValue("code") == 200) {
                    // Step 5: Show restore progress
                    log.info("Step 5: Show restore progress");
                    Thread.sleep(2000);
                    JSONObject showRestoreResult = showRestore(params);
                    steps.add(createStepResult("showRestore", showRestoreResult));
                }
            }

            result.put("steps", steps);
            result.put("snapshotName", snapshotName);
            
        } catch (Exception e) {
            log.error("Backup and restore process failed: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "Process execution failed: " + e.getMessage());
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

    private JSONObject errorResponse(String message) {
        JSONObject error = new JSONObject();
        error.put("code", 500);
        error.put("message", message);
        return error;
    }
}
