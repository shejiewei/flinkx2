package com.dtstack.flinkx.test;

import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.logminer.writer.DbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.Thread.sleep;

/**
 * Created by shejiewei on 2020/12/25.
 */
public class testT2 {

    public void insertSql() throws SQLException, InterruptedException {
        ClassUtil.forName("oracle.jdbc.driver.OracleDriver", getClass().getClassLoader());
        //默认关闭事务自动提交，手动控制事务

        Connection dbConn = DbUtil.getConnection("jdbc:oracle:thin:@120.78.216.6:9019/helowin", "kafka", "kafka");
        Statement statement = dbConn.createStatement();
        dbConn.setAutoCommit(false);

        long i = 100;
        while (i<=1000000) {

            sleep(1);

            String sql1 = "insert into \"KAFKA\".\"T2\"(\"ID\",\"NAME\") values ("+i+","+i+")";
           
           
            i++;
            statement.addBatch(sql1);
            if(i%100 == 0) {
            	 String sql2 = "update \"KAFKA\".\"T2\" set \"NAME\" = '" + (i + 2) + " ' where \"ID\" = " + i;
            	statement.addBatch(sql2);
            }
            if(i%101 == 0) {
            	String sql3 = " delete from \"KAFKA\".\"T2\" ";
            	statement.addBatch(sql3);
            }
            statement.executeBatch();
            DbUtil.commit(dbConn);
            statement.clearBatch();
        }
    }


    public static void main(String[] args) {
        testT2 test = new testT2();
        try {
            test.insertSql();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
