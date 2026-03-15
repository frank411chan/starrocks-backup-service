package com.starrocks.backup.controller;

import com.alibaba.fastjson2.JSONObject;
import com.starrocks.backup.service.StarRocksBackupService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * StarRocks 备份与恢复统一入口控制器
 * 
 * 入参结构（Java 8 命名规范）：
 * {
 *   "methodName": "backupAndRestore",
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
@Slf4j
@RestController
@RequestMapping("/api/starrocks")
@RequiredArgsConstructor
public class StarRocksBackupController {

    private final StarRocksBackupService backupService;

    /**
     * 统一入口方法
     * 
     * @param request 请求参数
     * @return 执行结果
     */
    @PostMapping("/execute")
    public JSONObject execute(@RequestBody Map<String, Object> request) {
        String methodName = (String) request.get("methodName");
        Map<String, Object> params = (Map<String, Object>) request.get("params");
        
        if (methodName == null || methodName.isEmpty()) {
            return errorResponse("methodName cannot be empty");
        }
        if (params == null) {
            return errorResponse("params cannot be empty");
        }

        log.info("Execute method: {}, params: {}", methodName, params);

        try {
            switch (methodName) {
                // 仓库管理
                case "createRepository":
                    return backupService.createRepository(params);
                case "showRepositories":
                    return backupService.showRepositories(params);

                // 表管理
                case "showTables":
                    return backupService.showTables(params);

                // 备份管理
                case "backupTable":
                    return backupService.backupTable(params);
                case "showBackup":
                    return backupService.showBackup(params);
                case "showSnapshot":
                    return backupService.showSnapshot(params);

                // 恢复管理
                case "restoreSnapshot":
                    return backupService.restoreSnapshot(params);
                case "showRestore":
                    return backupService.showRestore(params);

                // 整合方法
                case "backupAndRestore":
                    return backupService.backupAndRestore(params);

                default:
                    return errorResponse("Unknown method: " + methodName);
            }
        } catch (Exception e) {
            log.error("Execute method {} failed: {}", methodName, e.getMessage(), e);
            return errorResponse("Execution failed: " + e.getMessage());
        }
    }

    /**
     * 健康检查
     */
    @GetMapping("/health")
    public JSONObject health() {
        JSONObject result = new JSONObject();
        result.put("code", 200);
        result.put("message", "Service is running");
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }

    private JSONObject errorResponse(String message) {
        JSONObject error = new JSONObject();
        error.put("code", 400);
        error.put("message", message);
        return error;
    }
}
