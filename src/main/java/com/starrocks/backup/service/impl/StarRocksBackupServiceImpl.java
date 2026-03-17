package com.starrocks.backup.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    private final ObjectMapper objectMapper;

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

    private ObjectNode successResponse(String message) {
        ObjectNode result = objectMapper.createObjectNode();
        result.put("code", 200);
        result.put("message", message);
        return result;
    }

    private ObjectNode errorResponse(String message) {
        ObjectNode error = objectMapper.createObjectNode();
        error.put("code", 500);
        error.put("message", message);
        return error;
    }

    // ==================== 仓库管理 ====================

    @Override
    public ObjectNode createRepository(Map<String, Object> params) {
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

            ObjectNode result = successResponse("Repository created successfully");
            result.put("repoName", repoName);
            return result;
        } catch (Exception e) {
            log.error("Create repository failed: {}", e.getMessage(), e);
            return errorResponse("Create repository failed: " + e.getMessage());
        }
    }

    @Override
    public ObjectNode showRepositories(Map<String, Object> params) {
        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList("SHOW REPOSITORIES");
            
            ObjectNode result = successResponse("Query successful");
            result.set("data", objectMapper.valueToTree(list));
            result.put("total", list.size());
            return result;
        } catch (Exception e) {
            log.error("Query repositories failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }
    }

    // ==================== 表管理 ====================

    @Override
    public ObjectNode showTables(Map<String, Object> params) {
        String databaseName = getStringParam(params, "databaseName", "default_cluster");

        try {
            String sql = "SHOW TABLES FROM " + databaseName;
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            ObjectNode result = successResponse("Query successful");
            result.put("databaseName", databaseName);
            result.set("data", objectMapper.valueToTree(list));
            result.put("total", list.size());
            return result;
        } catch (Exception e) {
            log.error("Query tables failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }
    }

    // ==================== 备份管理 ====================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ObjectNode backupTable(Map<String, Object> params) {
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

            ObjectNode result = successResponse("Backup task submitted");
            result.put("snapshotName", snapshotName);
            result.put("databaseName", databaseName);
            result.put("tableName", tableName);
            result.put("byPartition", byPartition);
            result.set("partitionName", objectMapper.valueToTree(partitionNames));
            return result;
        } catch (Exception e) {
            log.error("Backup failed: {}", e.getMessage(), e);
            return errorResponse("Backup failed: " + e.getMessage());
        }
    }

    @Override
    public ObjectNode showBackup(Map<String, Object> params) {
        String databaseName = getStringParam(params, "databaseName");
        String sql = (databaseName != null && !databaseName.isEmpty()) 
            ? "SHOW BACKUP FROM " + databaseName 
            : "SHOW BACKUP";

        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            ObjectNode result = successResponse("Query successful");
            result.set("data", objectMapper.valueToTree(list));
            return result;
        } catch (Exception e) {
            log.error("Query backup progress failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }
    }

    @Override
    public ObjectNode showSnapshot(Map<String, Object> params) {
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
            
            ObjectNode result = successResponse("Query successful");
            result.set("data", objectMapper.valueToTree(list));
            return result;
        } catch (Exception e) {
            log.error("Query snapshot failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }
    }

    // ==================== 恢复管理 ====================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ObjectNode restoreSnapshot(Map<String, Object> params) {
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

            ObjectNode result = successResponse("Restore task submitted");
            result.put("snapshotName", snapshotName);
            result.put("databaseName", databaseName);
            result.put("tableName", tableName);
            result.put("newTableName", newTableName);
            result.put("restoreClusterId", restoreClusterId);
            return result;
        } catch (Exception e) {
            log.error("Restore failed: {}", e.getMessage(), e);
            return errorResponse("Restore failed: " + e.getMessage());
        }
    }

    @Override
    public ObjectNode showRestore(Map<String, Object> params) {
        String databaseName = getStringParam(params, "databaseName");
        String sql = (databaseName != null && !databaseName.isEmpty()) 
            ? "SHOW RESTORE FROM " + databaseName 
            : "SHOW RESTORE";

        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            ObjectNode result = successResponse("Query successful");
            result.set("data", objectMapper.valueToTree(list));
            return result;
        } catch (Exception e) {
            log.error("Query restore progress failed: {}", e.getMessage(), e);
            return errorResponse("Query failed: " + e.getMessage());
        }
    }

    // ==================== 整合方法 ====================

    @Override
    public ObjectNode backupAndRestore(Map<String, Object> params) {
        ObjectNode result = successResponse("Starting backup and restore process");
        ArrayNode steps = objectMapper.createArrayNode();

        try {
            // Step 1: Backup table
            log.info("Step 1: Backup table");
            ObjectNode backupResult = backupTable(params);
            steps.add(createStepResult("backupTable", backupResult));
            
            if (backupResult.get("code").asInt() != 200) {
                result.put("code", 500);
                result.put("message", "Backup failed, process terminated");
                result.set("steps", steps);
                return result;
            }

            String snapshotName = backupResult.get("snapshotName").asText();

            // Step 2: Show backup progress
            log.info("Step 2: Show backup progress");
            Thread.sleep(2000);
            ObjectNode showBackupResult = showBackup(params);
            steps.add(createStepResult("showBackup", showBackupResult));

            // Step 3: Show snapshot
            log.info("Step 3: Show snapshot");
            params.put("snapshotName", snapshotName);
            ObjectNode showSnapshotResult = showSnapshot(params);
            steps.add(createStepResult("showSnapshot", showSnapshotResult));

            // Step 4: Restore snapshot (if target cluster specified)
            String restoreClusterId = getStringParam(params, "restoreClusterId");
            if (restoreClusterId != null && !restoreClusterId.isEmpty()) {
                log.info("Step 4: Restore snapshot to cluster: {}", restoreClusterId);
                ObjectNode restoreResult = restoreSnapshot(params);
                steps.add(createStepResult("restoreSnapshot", restoreResult));

                if (restoreResult.get("code").asInt() == 200) {
                    // Step 5: Show restore progress
                    log.info("Step 5: Show restore progress");
                    Thread.sleep(2000);
                    ObjectNode showRestoreResult = showRestore(params);
                    steps.add(createStepResult("showRestore", showRestoreResult));
                }
            }

            result.set("steps", steps);
            result.put("snapshotName", snapshotName);
            
        } catch (Exception e) {
            log.error("Backup and restore process failed: {}", e.getMessage(), e);
            result.put("code", 500);
            result.put("message", "Process execution failed: " + e.getMessage());
            result.set("steps", steps);
        }

        return result;
    }

    private ObjectNode createStepResult(String stepName, ObjectNode result) {
        ObjectNode step = objectMapper.createObjectNode();
        step.put("step", stepName);
        step.put("code", result.get("code").asInt());
        step.put("message", result.get("message").asText());
        step.put("timestamp", System.currentTimeMillis());
        return step;
    }
}
