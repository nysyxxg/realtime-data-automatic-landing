package com.data.auto.landing.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnUtil {
    
    public static Connection getConnection(String driver, String url,
                                           String username, String password) {
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, username, password);
            System.out.println("初始化数据库连接....");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    
    public static void closeConnection(Connection newConnect){
        if(newConnect != null){
            try {
                System.out.println("关闭数据库连接....");
                newConnect.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
