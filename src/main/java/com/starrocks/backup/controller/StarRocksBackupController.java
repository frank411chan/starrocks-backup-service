package com.starrocks.backup.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.starrocks.backup.service.StarRocksBackupService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
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
    private final ObjectMapper objectMapper;

    /**
     * 统一入口方法
     * 
     * @param request 请求参数
     * @return 执行结果
     */
    @PostMapping("/execute")
    public ObjectNode execute(@RequestBody @Valid ExecuteRequest request) {
        String methodName = request.getMethodName();
        Map<String, Object> params = request.getParams();
        
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
    public ObjectNode health() {
        ObjectNode result = objectMapper.createObjectNode();
        result.put("code", 200);
        result.put("message", "Service is running");
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }

    private ObjectNode errorResponse(String message) {
        ObjectNode error = objectMapper.createObjectNode();
        error.put("code", 400);
        error.put("message", message);
        return error;
    }

    /**
     * 请求参数对象（用于 Hibernate Validator 验证）
     */
    public static class ExecuteRequest {
        @NotBlank(message = "methodName cannot be empty")
        private String methodName;
        
        private Map<String, Object> params;

        public String getMethodName() {
            return methodName;
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }

        public Map<String, Object> getParams() {
            return params;
        }

        public void setParams(Map<String, Object> params) {
            this.params = params;
        }
    }
}
