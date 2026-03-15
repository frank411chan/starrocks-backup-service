package com.starrocks.backup;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * StarRocks 备份与恢复服务启动类
 */
@SpringBootApplication
public class StarRocksBackupApplication {

    public static void main(String[] args) {
        SpringApplication.run(StarRocksBackupApplication.class, args);
    }
}
