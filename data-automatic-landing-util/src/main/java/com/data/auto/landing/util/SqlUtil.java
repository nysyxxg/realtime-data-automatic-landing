package com.data.auto.landing.util;

import java.util.ArrayList;
import java.util.List;

public class SqlUtil {
    
    public static String getCreateDataBaseSql(String dbType, String dataBaseName) {
        String sql = "";
        if ("mysql".equalsIgnoreCase(dbType)) {
            sql = "CREATE DATABASE IF NOT EXISTS " + dataBaseName + " default charset utf8 COLLATE utf8_general_ci; ";
        } else if ("hive".equalsIgnoreCase(dbType)) {
            sql = "create database if not exists " + dataBaseName;
        }
        return sql;
    }
    
    public static String getCreateTableSql(String dbType, String tableName, List<String> fieldList) {
        
        StringBuffer sql = new StringBuffer();
        if ("mysql".equalsIgnoreCase(dbType)) {
            sql.append("CREATE table IF NOT EXISTS " + tableName + "( ").append("\r\n");
            for (String field : fieldList) {
                System.out.println("建表字段： " + field);
                sql.append(field).append("  varchar(100)  DEFAULT NULL ").append(",").append("\n");
            }
            sql.append("PRIMARY KEY (`msgid`,`uuId`,`dataVer`)").append("\n");
            sql.append(") default charset utf8 COLLATE utf8_general_ci; ");
            
        } else if ("hive".equalsIgnoreCase(dbType)) {
            sql.append("CREATE EXTERNAL TABLE IF NOT EXISTS " + tableName + "( ").append("\r\n");
            for (String field : fieldList) {
                System.out.println("建表字段： " + field);
                sql.append(field).append("  string  DEFAULT NULL ").append(",").append("\n");
            }
            sql.append("exclusive_time timestamp ").append("\n");
            sql.append(") PARTITIONED BY (ds string comment '分区字段') STORED AS PARQUET; ");
        }
        return sql.toString();
    }
    
    public static void main(String[] args) {
        String dbType = "hive";
        String tableName = "test";
        List<String> fieldList = new ArrayList<String>();
        fieldList.add("id");
        fieldList.add("name");
        fieldList.add("sex");
        fieldList.add("msgid");
        fieldList.add("uuId");
        fieldList.add("dataVer");
        String sql = SqlUtil.getCreateTableSql(dbType, tableName, fieldList);
        System.out.println(sql);
    }
}
