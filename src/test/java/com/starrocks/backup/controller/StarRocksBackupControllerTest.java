package com.starrocks.backup.controller;

import com.alibaba.fastjson2.JSONObject;
import com.starrocks.backup.service.StarRocksBackupService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

/**
 * StarRocksBackupController 单元测试
 */
@ExtendWith(MockitoExtension.class)
class StarRocksBackupControllerTest {

    @Mock
    private StarRocksBackupService backupService;

    @InjectMocks
    private StarRocksBackupController controller;

    private Map<String, Object> request;
    private Map<String, Object> params;

    @BeforeEach
    void setUp() {
        request = new HashMap<>();
        params = new HashMap<>();
    }

    @Test
    void testExecute_MissingMethodName() {
        request.put("params", params);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(400, result.getIntValue("code"));
        assertTrue(result.getString("message").contains("methodName"));
    }

    @Test
    void testExecute_MissingParams() {
        request.put("methodName", "showTables");
        
        JSONObject result = controller.execute(request);
        
        assertEquals(400, result.getIntValue("code"));
        assertTrue(result.getString("message").contains("params"));
    }

    @Test
    void testExecute_CreateRepository() {
        request.put("methodName", "createRepository");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        mockResult.put("message", "Success");
        
        when(backupService.createRepository(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).createRepository(params);
    }

    @Test
    void testExecute_ShowRepositories() {
        request.put("methodName", "showRepositories");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        mockResult.put("data", new java.util.ArrayList<>());
        
        when(backupService.showRepositories(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).showRepositories(params);
    }

    @Test
    void testExecute_ShowTables() {
        request.put("methodName", "showTables");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        mockResult.put("databaseName", "test_db");
        
        when(backupService.showTables(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).showTables(params);
    }

    @Test
    void testExecute_BackupTable() {
        request.put("methodName", "backupTable");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        mockResult.put("snapshotName", "snapshot_001");
        
        when(backupService.backupTable(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).backupTable(params);
    }

    @Test
    void testExecute_ShowBackup() {
        request.put("methodName", "showBackup");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        
        when(backupService.showBackup(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).showBackup(params);
    }

    @Test
    void testExecute_ShowSnapshot() {
        request.put("methodName", "showSnapshot");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        
        when(backupService.showSnapshot(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).showSnapshot(params);
    }

    @Test
    void testExecute_RestoreSnapshot() {
        request.put("methodName", "restoreSnapshot");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        
        when(backupService.restoreSnapshot(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).restoreSnapshot(params);
    }

    @Test
    void testExecute_ShowRestore() {
        request.put("methodName", "showRestore");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        
        when(backupService.showRestore(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).showRestore(params);
    }

    @Test
    void testExecute_BackupAndRestore() {
        request.put("methodName", "backupAndRestore");
        request.put("params", params);
        
        JSONObject mockResult = new JSONObject();
        mockResult.put("code", 200);
        mockResult.put("steps", new java.util.ArrayList<>());
        
        when(backupService.backupAndRestore(anyMap())).thenReturn(mockResult);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(200, result.getIntValue("code"));
        verify(backupService).backupAndRestore(params);
    }

    @Test
    void testExecute_UnknownMethod() {
        request.put("methodName", "unknownMethod");
        request.put("params", params);
        
        JSONObject result = controller.execute(request);
        
        assertEquals(400, result.getIntValue("code"));
        assertTrue(result.getString("message").contains("Unknown method"));
    }

    @Test
    void testExecute_Exception() {
        request.put("methodName", "backupTable");
        request.put("params", params);
        
        when(backupService.backupTable(anyMap()))
            .thenThrow(new RuntimeException("Database connection failed"));
        
        JSONObject result = controller.execute(request);
        
        assertEquals(500, result.getIntValue("code"));
        assertTrue(result.getString("message").contains("Execution failed"));
    }

    @Test
    void testHealth() {
        JSONObject result = controller.health();
        
        assertEquals(200, result.getIntValue("code"));
        assertEquals("Service is running", result.getString("message"));
        assertNotNull(result.getLong("timestamp"));
    }
}
