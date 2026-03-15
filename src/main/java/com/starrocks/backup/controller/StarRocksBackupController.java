package com.starrocks.backup.controller;

import com.alibaba.fastjson2.JSONObject;
import com.starrocks.backup.service.StarRocksBackupService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * StarRocks 备份与恢复统一入口控制器
 * 通过方法名路由到不同的业务方法
 */
@Slf4j
@RestController
@RequestMapping("/api/starrocks")
@RequiredArgsConstructor
public class StarRocksBackupController {

    private final StarRocksBackupService backupService;

    /**
     * 统一入口方法
     * 根据 method 参数路由到不同的方法
     *
     * @param request 请求参数，必须包含 method 字段
     * @return 执行结果
     */
    @PostMapping("/execute")
    public JSONObject execute(@RequestBody Map<String, Object> request) {
        String method = (String) request.get("method");
        if (method == null || method.isEmpty()) {
            JSONObject error = new JSONObject();
            error.put("code", 400);
            error.put("message", "method 参数不能为空");
            return error;
        }

        log.info("执行方法: {}, 参数: {}", method, request);

        try {
            switch (method) {
                // 仓库管理
                case "createRepository":
                    return backupService.createRepository(request);
                case "showRepositories":
                    return backupService.showRepositories(request);

                // 表管理
                case "showTables":
                    return backupService.showTables(request);

                // 备份管理
                case "backupTable":
                    return backupService.backupTable(request);
                case "showBackup":
                    return backupService.showBackup(request);
                case "showSnapshot":
                    return backupService.showSnapshot(request);

                // 恢复管理
                case "restoreSnapshot":
                    return backupService.restoreSnapshot(request);
                case "showRestore":
                    return backupService.showRestore(request);

                // 整合方法
                case "backupAndRestore":
                    return backupService.backupAndRestore(request);

                default:
                    JSONObject error = new JSONObject();
                    error.put("code", 400);
                    error.put("message", "未知的方法: " + method);
                    return error;
            }
        } catch (Exception e) {
            log.error("执行方法 {} 失败: {}", method, e.getMessage(), e);
            JSONObject error = new JSONObject();
            error.put("code", 500);
            error.put("message", "执行失败: " + e.getMessage());
            return error;
        }
    }

    /**
     * 健康检查
     */
    @GetMapping("/health")
    public JSONObject health() {
        JSONObject result = new JSONObject();
        result.put("code", 200);
        result.put("message", "服务正常");
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
}
