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
 * 入参结构：
 * {
 *   "方法名": "备份与恢复表",
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
        String methodName = (String) request.get("方法名");
        Map<String, Object> params = (Map<String, Object>) request.get("参数");
        
        if (methodName == null || methodName.isEmpty()) {
            return errorResponse("方法名 不能为空");
        }
        if (params == null) {
            return errorResponse("参数 不能为空");
        }

        log.info("执行方法: {}, 参数: {}", methodName, params);

        try {
            switch (methodName) {
                // 仓库管理
                case "创建HDFS仓库":
                    return backupService.createRepository(params);
                case "显示仓库列表":
                    return backupService.showRepositories(params);

                // 表管理
                case "显示表":
                    return backupService.showTables(params);

                // 备份管理
                case "备份表":
                    return backupService.backupTable(params);
                case "查看备份进度":
                    return backupService.showBackup(params);
                case "查看备份快照":
                    return backupService.showSnapshot(params);

                // 恢复管理
                case "恢复快照":
                    return backupService.restoreSnapshot(params);
                case "查看恢复进度":
                    return backupService.showRestore(params);

                // 整合方法
                case "备份与恢复表":
                    return backupService.backupAndRestore(params);

                default:
                    return errorResponse("未知的方法: " + methodName);
            }
        } catch (Exception e) {
            log.error("执行方法 {} 失败: {}", methodName, e.getMessage(), e);
            return errorResponse("执行失败: " + e.getMessage());
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

    private JSONObject errorResponse(String message) {
        JSONObject error = new JSONObject();
        error.put("code", 400);
        error.put("message", message);
        return error;
    }
}
