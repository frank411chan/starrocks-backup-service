package com.starrocks.backup.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * StarRocksBackupServiceImpl 单元测试
 */
@ExtendWith(MockitoExtension.class)
class StarRocksBackupServiceImplTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private StarRocksBackupServiceImpl backupService;

    private Map<String, Object> params;

    @BeforeEach
    void setUp() {
        params = new HashMap<>();
    }

    // ==================== 参数解析测试 ====================

    @Test
    void testGetStringParam_WithValue() {
        params.put("databaseName", "test_db");
        
        ObjectNode result = backupService.showTables(params);
        
        verify(jdbcTemplate).queryForList(eq("SHOW TABLES FROM test_db"));
    }

    @Test
    void testGetStringParam_WithDefault() {
        ObjectNode result = backupService.showTables(params);
        
        verify(jdbcTemplate).queryForList(eq("SHOW TABLES FROM default_cluster"));
    }

    @Test
    void testGetPartitionNames_SingleString() {
        params.put("partitionName", "p20260315");
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        params.put("byPartition", true);
        
        backupService.backupTable(params);
        
        verify(jdbcTemplate).execute(contains("PARTITION (p20260315)"));
    }

    @Test
    void testGetPartitionNames_MultipleList() {
        List<String> partitions = Arrays.asList("p20260315", "p20260316");
        params.put("partitionName", partitions);
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        params.put("byPartition", true);
        
        backupService.backupTable(params);
        
        verify(jdbcTemplate).execute(contains("PARTITION (p20260315, p20260316)"));
    }

    @Test
    void testIsByPartition_True() {
        params.put("byPartition", true);
        params.put("partitionName", "p1");
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        
        backupService.backupTable(params);
        
        verify(jdbcTemplate).execute(contains("PARTITION"));
    }

    @Test
    void testIsByPartition_False() {
        params.put("byPartition", false);
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        
        backupService.backupTable(params);
        
        verify(jdbcTemplate).execute(not(contains("PARTITION")));
    }

    // ==================== 仓库管理测试 ====================

    @Test
    void testCreateRepository_Success() {
        params.put("repoName", "hdfs_repo");
        params.put("hdfsPath", "hdfs://namenode:9000/backup");
        params.put("brokerName", "hdfs_broker");
        params.put("username", "admin");
        params.put("password", "password123");
        
        doNothing().when(jdbcTemplate).execute(anyString());
        
        ObjectNode result = backupService.createRepository(params);
        
        assertEquals(200, result.get("code").asInt());
        assertEquals("hdfs_repo", result.get("repoName").asText());
        verify(jdbcTemplate).execute(contains("CREATE REPOSITORY"));
    }

    @Test
    void testCreateRepository_MissingParams() {
        params.put("repoName", "hdfs_repo");
        // missing hdfsPath
        
        ObjectNode result = backupService.createRepository(params);
        
        assertEquals(500, result.get("code").asInt());
        assertTrue(result.get("message").asText().contains("cannot be empty"));
    }

    @Test
    void testCreateRepository_Exception() {
        params.put("repoName", "hdfs_repo");
        params.put("hdfsPath", "hdfs://namenode:9000/backup");
        params.put("username", "admin");
        params.put("password", "password123");
        
        doThrow(new RuntimeException("Connection refused"))
            .when(jdbcTemplate).execute(anyString());
        
        ObjectNode result = backupService.createRepository(params);
        
        assertEquals(500, result.get("code").asInt());
        assertTrue(result.get("message").asText().contains("Connection refused"));
    }

    @Test
    void testShowRepositories_Success() {
        List<Map<String, Object>> mockList = new ArrayList<>();
        Map<String, Object> repo = new HashMap<>();
        repo.put("RepoId", 1);
        repo.put("RepoName", "hdfs_repo");
        mockList.add(repo);
        
        when(jdbcTemplate.queryForList("SHOW REPOSITORIES")).thenReturn(mockList);
        
        ObjectNode result = backupService.showRepositories(params);
        
        assertEquals(200, result.get("code").asInt());
        assertEquals(1, result.get("total").asInt());
        assertNotNull(result.get("data"));
    }

    // ==================== 表管理测试 ====================

    @Test
    void testShowTables_Success() {
        params.put("databaseName", "test_db");
        
        List<Map<String, Object>> mockList = new ArrayList<>();
        Map<String, Object> table = new HashMap<>();
        table.put("TableName", "table1");
        mockList.add(table);
        
        when(jdbcTemplate.queryForList("SHOW TABLES FROM test_db")).thenReturn(mockList);
        
        ObjectNode result = backupService.showTables(params);
        
        assertEquals(200, result.get("code").asInt());
        assertEquals("test_db", result.get("databaseName").asText());
        assertEquals(1, result.get("total").asInt());
    }

    // ==================== 备份管理测试 ====================

    @Test
    void testBackupTable_Success() {
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        params.put("snapshotName", "snapshot_001");
        params.put("byPartition", false);
        
        doNothing().when(jdbcTemplate).execute(anyString());
        
        ObjectNode result = backupService.backupTable(params);
        
        assertEquals(200, result.get("code").asInt());
        assertEquals("snapshot_001", result.get("snapshotName").asText());
        verify(jdbcTemplate).execute(contains("BACKUP SNAPSHOT"));
    }

    @Test
    void testBackupTable_AutoGenerateSnapshotName() {
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        // snapshotName not provided
        
        doNothing().when(jdbcTemplate).execute(anyString());
        
        ObjectNode result = backupService.backupTable(params);
        
        assertEquals(200, result.get("code").asInt());
        assertNotNull(result.get("snapshotName"));
        assertTrue(result.get("snapshotName").asText().startsWith("db1_table1_"));
    }

    @Test
    void testBackupTable_MissingParams() {
        params.put("databaseName", "db1");
        // missing tableName and repoName
        
        ObjectNode result = backupService.backupTable(params);
        
        assertEquals(500, result.get("code").asInt());
    }

    @Test
    void testShowBackup_Success() {
        params.put("databaseName", "db1");
        
        List<Map<String, Object>> mockList = new ArrayList<>();
        when(jdbcTemplate.queryForList("SHOW BACKUP FROM db1")).thenReturn(mockList);
        
        ObjectNode result = backupService.showBackup(params);
        
        assertEquals(200, result.get("code").asInt());
        verify(jdbcTemplate).queryForList("SHOW BACKUP FROM db1");
    }

    @Test
    void testShowSnapshot_WithSnapshotName() {
        params.put("repoName", "repo1");
        params.put("snapshotName", "snapshot_001");
        
        List<Map<String, Object>> mockList = new ArrayList<>();
        when(jdbcTemplate.queryForList(contains("WHERE SNAPSHOT"))).thenReturn(mockList);
        
        ObjectNode result = backupService.showSnapshot(params);
        
        assertEquals(200, result.get("code").asInt());
    }

    // ==================== 恢复管理测试 ====================

    @Test
    void testRestoreSnapshot_Success() {
        params.put("databaseName", "db1");
        params.put("snapshotName", "snapshot_001");
        params.put("repoName", "repo1");
        params.put("tableName", "table1");
        params.put("newTableName", "table1_backup");
        params.put("backupTimestamp", "2024-03-15-10-30-00");
        params.put("restoreClusterId", "cluster2");
        
        doNothing().when(jdbcTemplate).execute(anyString());
        
        ObjectNode result = backupService.restoreSnapshot(params);
        
        assertEquals(200, result.get("code").asInt());
        assertEquals("table1_backup", result.get("newTableName").asText());
        verify(jdbcTemplate).execute(contains("RESTORE SNAPSHOT"));
        verify(jdbcTemplate).execute(contains("cluster2"));
    }

    @Test
    void testRestoreSnapshot_WithoutNewTableName() {
        params.put("databaseName", "db1");
        params.put("snapshotName", "snapshot_001");
        params.put("repoName", "repo1");
        params.put("tableName", "table1");
        
        doNothing().when(jdbcTemplate).execute(anyString());
        
        ObjectNode result = backupService.restoreSnapshot(params);
        
        assertEquals(200, result.get("code").asInt());
        verify(jdbcTemplate).execute(not(contains("AS")));
    }

    // ==================== 整合方法测试 ====================

    @Test
    void testBackupAndRestore_Success() throws InterruptedException {
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        params.put("restoreClusterId", "cluster2");
        params.put("byPartition", false);
        
        doNothing().when(jdbcTemplate).execute(anyString());
        when(jdbcTemplate.queryForList(anyString())).thenReturn(new ArrayList<>());
        
        ObjectNode result = backupService.backupAndRestore(params);
        
        assertEquals(200, result.get("code").asInt());
        assertNotNull(result.get("steps"));
        assertNotNull(result.get("snapshotName"));
    }

    @Test
    void testBackupAndRestore_BackupFailed() {
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        
        doThrow(new RuntimeException("Backup failed"))
            .when(jdbcTemplate).execute(contains("BACKUP SNAPSHOT"));
        
        ObjectNode result = backupService.backupAndRestore(params);
        
        assertEquals(500, result.get("code").asInt());
        assertTrue(result.get("message").asText().contains("Backup failed"));
    }

    @Test
    void testBackupAndRestore_WithoutRestoreCluster() throws InterruptedException {
        params.put("databaseName", "db1");
        params.put("tableName", "table1");
        params.put("repoName", "repo1");
        // no restoreClusterId
        
        doNothing().when(jdbcTemplate).execute(anyString());
        when(jdbcTemplate.queryForList(anyString())).thenReturn(new ArrayList<>());
        
        ObjectNode result = backupService.backupAndRestore(params);
        
        assertEquals(200, result.get("code").asInt());
        // Should only have 3 steps (backup, showBackup, showSnapshot)
        assertEquals(3, result.get("steps").size());
    }
}
