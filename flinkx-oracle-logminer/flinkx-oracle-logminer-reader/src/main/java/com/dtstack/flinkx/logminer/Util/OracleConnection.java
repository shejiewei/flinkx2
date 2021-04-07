package com.dtstack.flinkx.logminer.Util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by shejiewei on 2020/12/11.
 */
public class OracleConnection{    
    
    public Connection connect(String url,String userName,String password) throws SQLException{
        return DriverManager.getConnection(
            url,
            userName,
            password);
    }
}