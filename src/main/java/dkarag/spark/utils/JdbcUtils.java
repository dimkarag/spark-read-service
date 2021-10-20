package dkarag.spark.utils;

import dkarag.spark.configs.JdbcConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class JdbcUtils {

    public Long rowsOfTable(String tableName, JdbcConfig jdbcConfig){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(createDriverManagerDataSource(jdbcConfig));
        String query = "select count(*) as count from " + tableName;
        return Long.valueOf(String.valueOf(jdbcTemplate.queryForList(query).get(0).get("count")));
    }
    public Long upperBoundOfColumn(String columnName, String tableName, JdbcConfig jdbcConfig){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(createDriverManagerDataSource(jdbcConfig));
        String query = "select max(" + columnName + ") as max from " + tableName;
        return Long.valueOf(String.valueOf(jdbcTemplate.queryForList(query).get(0).get("max")));
    }
    public Long lowerBoundOfColumn(String columnName, String tableName, JdbcConfig jdbcConfig){
        JdbcTemplate jdbcTemplate = new JdbcTemplate(createDriverManagerDataSource(jdbcConfig));
        String query = "select min(" + columnName + ") as min from " + tableName;
        return Long.valueOf(String.valueOf(jdbcTemplate.queryForList(query).get(0).get("max")));
    }

    private DriverManagerDataSource createDriverManagerDataSource(JdbcConfig jdbcConfig) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(jdbcConfig.getUrl());
        dataSource.setUsername(jdbcConfig.getUsername());
        dataSource.setPassword(jdbcConfig.getPassword());
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        return dataSource;
    }
}
