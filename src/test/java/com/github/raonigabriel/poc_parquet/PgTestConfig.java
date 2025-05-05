package com.github.raonigabriel.poc_parquet;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import liquibase.integration.spring.SpringLiquibase;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class PgTestConfig {
    
    @Container
    @SuppressWarnings({"rawTypes", "resource"})
    private static final PostgreSQLContainer<?> PG_CONTAINER = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres");


    @Bean
    public DataSource dataSource() {
        PG_CONTAINER.start();
        log.info("PostgreSQL container started: {}", PG_CONTAINER.getJdbcUrl());
        final var dataSourceBuilder = new HikariConfig();
        dataSourceBuilder.setDriverClassName(PG_CONTAINER.getDriverClassName());
        dataSourceBuilder.setJdbcUrl(PG_CONTAINER.getJdbcUrl());
        dataSourceBuilder.setUsername(PG_CONTAINER.getUsername());
        dataSourceBuilder.setPassword(PG_CONTAINER.getPassword());
        return new HikariDataSource(dataSourceBuilder);
    }

    @Bean
    public SpringLiquibase liquibase(DataSource dataSource) {
        final var liquibase = new SpringLiquibase();
        liquibase.setDataSource(dataSource);
        liquibase.setDefaultSchema("public");
        liquibase.setChangeLog("classpath:db/migrations/changeLog.yml");
        return liquibase;
    }
}
