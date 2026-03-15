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
 * 参数说明：
 * - 备份机房id: 源集群标识，如 "ht"
 * - 恢复机房id: 目标集群标识，如 "db"
 * - 数据库名: 数据库名称，如 "lmp_label"
 * - 表名: 表名称，如 "table1"
 * - byPartition: 是否按分区备份，true/false
 * - 分区名: 分区名称，如 "p20260315"
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StarRocksBackupServiceImpl implements StarRocksBackupService {

    private final JdbcTemplate jdbcTemplate;

    // ==================== 参数解析工具方法 ====================

    private String getParam(Map<String, Object> params, String key, String defaultValue) {
        Object value = params.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private String getParam(Map<String, Object> params, String key) {
        return getParam(params, key, null);
    }

    private List<String> getPartitions(Map<String, Object> params) {
        List<String> partitions = new ArrayList<>();
        
        // 支持单分区或多分区
        Object partitionObj = params.get("分区名");
        if (partitionObj instanceof List) {
            partitions = (List<String>) partitionObj;
        } else if (partitionObj instanceof String) {
            String partitionStr = (String) partitionObj;
            if (!partitionStr.isEmpty()) {
                // 支持逗号分隔的多分区
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
        
        String repoName = getParam(params, "仓库名");
        String location = getParam(params, "HDFS路径");
        String broker = getParam(params, "broker名", "hdfs_broker");
        String username = getParam(params, "用户名");
        String password = getParam(params, "密码");

        if (repoName == null || location == null) {
            return errorResponse("仓库名 和 HDFS路径 不能为空");
        }

        try {
            String sql = String.format(
                "CREATE REPOSITORY `%s` WITH BROKER `%s` ON LOCATION \"%s\" PROPERTIES (\"username\"=\"%s\",\"password\"=\"%s\")",
                repoName, broker, location, username, password
            );

            log.info("执行SQL: {}", sql);
            jdbcTemplate.execute(sql);

            result.put("code", 200);
            result.put("message", "仓库创建成功");
            result.put("仓库名", repoName);
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
            List<Map<String, Object>> list = jdbcTemplate.queryForList("SHOW REPOSITORIES");
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("data", list);
            result.put("total", list.size());
        } catch (Exception e) {
            log.error("查询仓库失败: {}", e.getMessage(), e);
            return errorResponse("查询失败: " + e.getMessage());
        }

        return result;
    }

    // ==================== 表管理 ====================

    @Override
    public JSONObject showTables(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = getParam(params, "数据库名", "default_cluster");

        try {
            String sql = "SHOW TABLES FROM " + database;
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("数据库名", database);
            result.put("data", list);
            result.put("total", list.size());
        } catch (Exception e) {
            log.error("查询表失败: {}", e.getMessage(), e);
            return errorResponse("查询失败: " + e.getMessage());
        }

        return result;
    }

    // ==================== 备份管理 ====================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public JSONObject backupTable(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = getParam(params, "数据库名");
        String tableName = getParam(params, "表名");
        String repoName = getParam(params, "仓库名");
        String snapshotName = getParam(params, "快照名");
        
        // 分区相关
        boolean byPartition = isByPartition(params);
        List<String> partitions = getPartitions(params);

        if (database == null || tableName == null || repoName == null) {
            return errorResponse("数据库名、表名、仓库名 不能为空");
        }

        // 自动生成快照名
        if (snapshotName == null || snapshotName.isEmpty()) {
            snapshotName = String.format("%s_%s_%d", database, tableName, System.currentTimeMillis());
        }

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("BACKUP SNAPSHOT ").append(database).append(".").append(snapshotName);
            sql.append(" TO `").append(repoName).append("` ");
            sql.append("ON (").append(tableName);
            
            // 如果按分区备份且指定了分区
            if (byPartition && !partitions.isEmpty()) {
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
            result.put("快照名", snapshotName);
            result.put("数据库名", database);
            result.put("表名", tableName);
            result.put("byPartition", byPartition);
            result.put("分区名", partitions);
        } catch (Exception e) {
            log.error("备份失败: {}", e.getMessage(), e);
            return errorResponse("备份失败: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showBackup(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = getParam(params, "数据库名");
        String sql = (database != null && !database.isEmpty()) 
            ? "SHOW BACKUP FROM " + database 
            : "SHOW BACKUP";

        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("data", list);
        } catch (Exception e) {
            log.error("查询备份进度失败: {}", e.getMessage(), e);
            return errorResponse("查询失败: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showSnapshot(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String repoName = getParam(params, "仓库名");
        String snapshotName = getParam(params, "快照名");

        if (repoName == null) {
            return errorResponse("仓库名 不能为空");
        }

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
            return errorResponse("查询失败: " + e.getMessage());
        }

        return result;
    }

    // ==================== 恢复管理 ====================

    @Override
    @Transactional(rollbackFor = Exception.class)
    public JSONObject restoreSnapshot(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = getParam(params, "数据库名");
        String snapshotName = getParam(params, "快照名");
        String repoName = getParam(params, "仓库名");
        String tableName = getParam(params, "表名");
        String newTableName = getParam(params, "新表名");
        String backupTimestamp = getParam(params, "备份时间戳");
        String targetCluster = getParam(params, "恢复机房id");

        if (database == null || snapshotName == null || repoName == null || tableName == null) {
            return errorResponse("数据库名、快照名、仓库名、表名 不能为空");
        }

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("RESTORE SNAPSHOT ").append(database).append(".").append(snapshotName);
            sql.append(" FROM `").append(repoName).append("` ");
            sql.append("ON (").append(tableName);
            
            // 如果指定了新表名（恢复到不同集群或重命名）
            if (newTableName != null && !newTableName.isEmpty()) {
                sql.append(" AS ").append(newTableName);
            }
            
            sql.append(") ");
            sql.append("PROPERTIES (");
            
            if (backupTimestamp != null && !backupTimestamp.isEmpty()) {
                sql.append("\"backup_timestamp\"=\"").append(backupTimestamp).append("\"");
            }
            
            if (targetCluster != null && !targetCluster.isEmpty()) {
                if (backupTimestamp != null) sql.append(", ");
                sql.append("\"cluster\"=\"").append(targetCluster).append("\"");
            }
            
            sql.append(")");

            log.info("执行恢复SQL: {}", sql);
            jdbcTemplate.execute(sql.toString());

            result.put("code", 200);
            result.put("message", "恢复任务已提交");
            result.put("快照名", snapshotName);
            result.put("数据库名", database);
            result.put("表名", tableName);
            result.put("新表名", newTableName);
            result.put("恢复机房id", targetCluster);
        } catch (Exception e) {
            log.error("恢复失败: {}", e.getMessage(), e);
            return errorResponse("恢复失败: " + e.getMessage());
        }

        return result;
    }

    @Override
    public JSONObject showRestore(Map<String, Object> params) {
        JSONObject result = new JSONObject();
        
        String database = getParam(params, "数据库名");
        String sql = (database != null && !database.isEmpty()) 
            ? "SHOW RESTORE FROM " + database 
            : "SHOW RESTORE";

        try {
            List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
            
            result.put("code", 200);
            result.put("message", "查询成功");
            result.put("data", list);
        } catch (Exception e) {
            log.error("查询恢复进度失败: {}", e.getMessage(), e);
            return errorResponse("查询失败: " + e.getMessage());
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
            steps.add(createStepResult("备份表", backupResult));
            
            if (backupResult.getIntValue("code") != 200) {
                result.put("code", 500);
                result.put("message", "备份失败，流程终止");
                result.put("steps", steps);
                return result;
            }

            String snapshotName = backupResult.getString("快照名");

            // 步骤2: 查看备份进度
            log.info("步骤2: 查看备份进度");
            Thread.sleep(2000);
            JSONObject showBackupResult = showBackup(params);
            steps.add(createStepResult("查看备份进度", showBackupResult));

            // 步骤3: 查看备份快照
            log.info("步骤3: 查看备份快照");
            params.put("快照名", snapshotName);
            JSONObject showSnapshotResult = showSnapshot(params);
            steps.add(createStepResult("查看备份快照", showSnapshotResult));

            // 步骤4: 恢复快照（如果指定了恢复机房）
            String targetCluster = getParam(params, "恢复机房id");
            if (targetCluster != null && !targetCluster.isEmpty()) {
                log.info("步骤4: 恢复快照到机房: {}", targetCluster);
                JSONObject restoreResult = restoreSnapshot(params);
                steps.add(createStepResult("恢复快照", restoreResult));

                if (restoreResult.getIntValue("code") == 200) {
                    // 步骤5: 查看恢复进度
                    log.info("步骤5: 查看恢复进度");
                    Thread.sleep(2000);
                    JSONObject showRestoreResult = showRestore(params);
                    steps.add(createStepResult("查看恢复进度", showRestoreResult));
                }
            }

            result.put("steps", steps);
            result.put("快照名", snapshotName);
            
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

    private JSONObject errorResponse(String message) {
        JSONObject error = new JSONObject();
        error.put("code", 500);
        error.put("message", message);
        return error;
    }
}
