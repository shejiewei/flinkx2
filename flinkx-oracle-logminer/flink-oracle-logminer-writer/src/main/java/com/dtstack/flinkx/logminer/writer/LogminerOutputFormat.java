/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.logminer.writer;

import ch.qos.logback.core.db.dialect.DBUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.outputformat.BaseRichOutputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.GsonUtil;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class LogminerOutputFormat extends BaseRichOutputFormat {

    protected String url;

    protected String username;

    protected String password;


    protected String driverName;

    protected  Connection dbConn;

    protected PreparedStatement preparedStatement;

    protected Statement statement;
    protected List<String> preSql;

    protected List<String> postSql;


    protected String mode = EWriteMode.INSERT.name();

    /**
     * just for postgresql,use copy replace insert
     */
    protected String insertSqlMode;

    protected String table;

    protected List<String> column;

    protected Map<String, List<String>> updateKey;

    protected List<String> fullColumn;

    protected List<String> fullColumnType;

    protected List<String> columnType = new ArrayList<>();

    protected int delaytime=30; //秒

    final int[] midTime = {0};

    protected Row lastRow = null;

    protected boolean readyCheckpoint;

    protected long rowsOfCurrentTransaction;

    public Properties properties;

    protected final static String CONN_CLOSE_ERROR_MSG = "No operations allowed";


    private static DruidDataSource ds = null;


    @Override
    public void openInternal(int taskNumber, int numTasks) throws IOException {
        try {

            InputStream in = DBUtil.class.getClassLoader()
                    .getResourceAsStream("druid.properties");
            Properties props = new Properties();
            props.load(in);
            props.setProperty("url",url);
            props.setProperty("username",username);
            props.setProperty("password",password);
            ds = (DruidDataSource) DruidDataSourceFactory.createDataSource(props);
           // ClassUtil.forName("oracle.jdbc.driver.OracleDriver", getClass().getClassLoader());
            dbConn =ds.getConnection();// DbUtil.getConnection(url, username, password);
            dbConn.setAutoCommit(false);  //默认关闭事务自动提交，手动控制事务

            readyCheckpoint = false;

            LOG.info("subTask[{}}] wait finished", taskNumber);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("创建数据库连接池失败,{}",e);
        }


            Timer timer = new Timer();
            midTime[0]=delaytime;
            timer.schedule(new TimerTask() {
                public void run() {
                    midTime[0]--;
                    long ss = midTime[0] % 60;
                   // LOG.warn("还剩" + ss + "秒");
                    if (midTime[0] <= 0) {
                        writeRecordInternal();  //oracle的批处理插入操作支持,能够在时间延时后插入数据
                        midTime[0] = delaytime;
                    }
                }
            }, 0, 1000);


    }


    @Override
    public void writeRecord(Row row) throws IOException {

            super.writeRecord(row);
            midTime[0]=delaytime;

            //超时自动提交
          /*  Calendar cal = Calendar.getInstance();
            if (beginTime==null)
            {
                beginTime=cal.getTime();
            }
            endTime = cal.getTime();
            long interval = diffMin(beginTime, endTime);
            if (interval>=delayInteval)
            {
                writeRecordInternal();
                beginTime=endTime;
            }
            else {
                beginTime=endTime;
            }*/
        }


    public static Connection openConnection() throws SQLException{
        Connection  dbConn= ds.getConnection();
        dbConn.setAutoCommit(false);  //默认关闭事务自动提交，手动控制事务
        return  dbConn;
    }

    protected void processWriteException(Exception e, int index, Row row) throws WriteRecordException {
        if (e instanceof SQLException) {
            if (e.getMessage().contains(CONN_CLOSE_ERROR_MSG)) {
                throw new RuntimeException("Connection maybe closed", e);
            }
        }

        if (index < row.getArity()) {
            String message = recordConvertDetailErrorMessage(index, row);
            LOG.error(message, e);
            throw new WriteRecordException(message, e, index, row);
        }
        throw new WriteRecordException(e.getMessage(), e);
    }

    @Override
    protected void writeSingleRecordInternal(Row row) {

        int index = 0;
        String sql = "";
        String message="";

        try {
            dbConn =ds.getConnection();
           // dbConn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Pattern p= Pattern.compile("(SQL_REDO=)([\\S\\s]*)(, OPERATION)");
        try {
            for (; index < row.getArity(); index++) {
                Object field = row.getField(0);
                Map<String, String> map = null;
                if (field instanceof HashMap) {
                    map = (Map<String, String>) field;
                    sql = map.get("SQL_REDO");
                }
                else if (field instanceof Map) {
                    Map fMap= (Map) field;
                     message =fMap.get("message").toString();
                    if (StringUtils.isNotBlank(message)) {
                        message = StringEscapeUtils.unescapeJava(message);
                        Matcher m = p.matcher(message);
                        if (m.find()) {
                            sql = m.group(2);
                        }
                    }
                }
            }
            if (StringUtils.isNotBlank(sql)) {
                preparedStatement = dbConn.prepareStatement(sql);
                preparedStatement.execute();
                LOG.warn("接收到的sql:{},url:{},插入数据", sql, url);
            }
            DbUtil.commit(dbConn);
        } catch (Exception e) {
            LOG.error("出错了,sql:{}撤回,错误:{},message:{}",sql,e,message);
         //   DbUtil.rollBack(dbConn);
            LOG.info("插入目标表失败,检查表结构是否一致,执行sql{},错误e:{}", sql, e);
           // processWriteException(e, index, row);
        }
        finally {
            if(preparedStatement!=null)
            {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (dbConn!=null)
            {
                try {
                    dbConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                ds.removeAbandoned();
            }
        }

    }

    public Connection connect(String url,String userName,String password) throws SQLException{
        return DriverManager.getConnection(
                url,
                userName,
                password);
    }

    @Override
    protected void writeMultipleRecordsInternal()  {

        try {
            dbConn =ds.getConnection();
          //  dbConn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            statement = dbConn.createStatement();
            Pattern p= Pattern.compile("(SQL_REDO\":\")([\\S\\s]*)(\",\"OPERATION)");
            for (Row row : rows) {
                for (int index = 0; index < row.getArity(); index++) {
                    Object field = row.getField(0);
                    Map<String, String> map = null;
                    String sql="";
                    if (field instanceof HashMap) {
                        map = (Map<String, String>) field;
                        sql = map.get("SQL_REDO");
                    }
                    else if (field instanceof Map) {
                        Map fMap= (Map) field;
                        String  message =fMap.get("message").toString();
                        message=StringEscapeUtils.unescapeJava(message);
                        Matcher m=p.matcher(message);
                        if(m.find()) {
                         sql=m.group(2);
                        }
                    }
                    if (StringUtils.isNotBlank(sql)) {
                        statement.addBatch(sql);
                        LOG.warn("接收到的sql:{},url:{},插入数据", sql, url);
                    }
                }

                if (restoreConfig.isRestore()) {
                    if (lastRow != null) {
                        readyCheckpoint = !ObjectUtils.equals(lastRow.getField(restoreConfig.getRestoreColumnIndex()),
                                row.getField(restoreConfig.getRestoreColumnIndex()));
                    }

                    lastRow = row;
                }
            }
            statement.executeBatch();
            if (restoreConfig.isRestore()) {
                rowsOfCurrentTransaction += rows.size();
            } else {
                //手动提交事务
                DbUtil.commit(dbConn);
            }
           // statement.clearBatch();
        } catch (Exception e) {
            LOG.warn("write Multiple Records error, row size = {}, first row = {},  e = {}",
                    rows.size(),
                    rows.size() > 0 ? GsonUtil.GSON.toJson(rows.get(0)) : "null",
                    ExceptionUtil.getErrorMessage(e));
            LOG.warn("error to writeMultipleRecords, start to rollback connection, e = {}", ExceptionUtil.getErrorMessage(e));
            //DbUtil.rollBack(dbConn);
            //throw e;   不能抛出错误,会导致整个程序崩溃
        } finally {
            //执行完后清空batch
            if (statement!=null) {
                try {
                    statement.clearBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (dbConn!=null)
            {
                try {
                    dbConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                ds.removeAbandoned();
            }
        }
    }


    @Override
    public void closeInternal() throws IOException {
        readyCheckpoint = false;
        boolean commit = true;
        try {
            numWriteCounter.add(rowsOfCurrentTransaction);
            String state = getTaskState();
            // Do not commit a transaction when the task is canceled or failed
            if (!RUNNING_STATE.equals(state) && restoreConfig.isRestore()) {
                commit = false;
            }
        } catch (Exception e) {
            LOG.error("Get task status error:{}", e.getMessage());
        }

        try {

            DbUtil.closeDbResources(null, preparedStatement, dbConn, commit);
            ds.close();
        }
        catch (Exception e){
            LOG.info("网络断了,{}",e.toString());
        }
        dbConn = null;
    }


}
