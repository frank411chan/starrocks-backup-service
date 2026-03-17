package com.starrocks.backup.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.starrocks.backup.service.StarRocksBackupService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
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

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private StarRocksBackupController controller;

    private StarRocksBackupController.ExecuteRequest request;

    @BeforeEach
    void setUp() {
        request = new StarRocksBackupController.ExecuteRequest();
    }

    @Test
    void testExecute_MissingMethodName() {
        request.setParams(new HashMap<>());
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(400, result.get("code").asInt());
        assertTrue(result.get("message").asText().contains("methodName"));
    }

    @Test
    void testExecute_MissingParams() {
        request.setMethodName("showTables");
        // params is null
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(400, result.get("code").asInt());
        assertTrue(result.get("message").asText().contains("params"));
    }

    @Test
    void testExecute_CreateRepository() {
        request.setMethodName("createRepository");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        mockResult.put("message", "Success");
        
        when(backupService.createRepository(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).createRepository(anyMap());
    }

    @Test
    void testExecute_ShowRepositories() {
        request.setMethodName("showRepositories");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        
        when(backupService.showRepositories(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).showRepositories(anyMap());
    }

    @Test
    void testExecute_ShowTables() {
        request.setMethodName("showTables");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        mockResult.put("databaseName", "test_db");
        
        when(backupService.showTables(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).showTables(anyMap());
    }

    @Test
    void testExecute_BackupTable() {
        request.setMethodName("backupTable");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        mockResult.put("snapshotName", "snapshot_001");
        
        when(backupService.backupTable(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).backupTable(anyMap());
    }

    @Test
    void testExecute_ShowBackup() {
        request.setMethodName("showBackup");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        
        when(backupService.showBackup(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).showBackup(anyMap());
    }

    @Test
    void testExecute_ShowSnapshot() {
        request.setMethodName("showSnapshot");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        
        when(backupService.showSnapshot(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).showSnapshot(anyMap());
    }

    @Test
    void testExecute_RestoreSnapshot() {
        request.setMethodName("restoreSnapshot");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        
        when(backupService.restoreSnapshot(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).restoreSnapshot(anyMap());
    }

    @Test
    void testExecute_ShowRestore() {
        request.setMethodName("showRestore");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        
        when(backupService.showRestore(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).showRestore(anyMap());
    }

    @Test
    void testExecute_BackupAndRestore() {
        request.setMethodName("backupAndRestore");
        request.setParams(new HashMap<>());
        
        ObjectNode mockResult = objectMapper.createObjectNode();
        mockResult.put("code", 200);
        mockResult.set("steps", objectMapper.createArrayNode());
        
        when(backupService.backupAndRestore(anyMap())).thenReturn(mockResult);
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(200, result.get("code").asInt());
        verify(backupService).backupAndRestore(anyMap());
    }

    @Test
    void testExecute_UnknownMethod() {
        request.setMethodName("unknownMethod");
        request.setParams(new HashMap<>());
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(400, result.get("code").asInt());
        assertTrue(result.get("message").asText().contains("Unknown method"));
    }

    @Test
    void testExecute_Exception() {
        request.setMethodName("backupTable");
        request.setParams(new HashMap<>());
        
        when(backupService.backupTable(anyMap()))
            .thenThrow(new RuntimeException("Database connection failed"));
        
        ObjectNode result = controller.execute(request);
        
        assertEquals(500, result.get("code").asInt());
        assertTrue(result.get("message").asText().contains("Execution failed"));
    }

    @Test
    void testHealth() {
        ObjectNode result = controller.health();
        
        assertEquals(200, result.get("code").asInt());
        assertEquals("Service is running", result.get("message").asText());
        assertNotNull(result.get("timestamp"));
    }
}
