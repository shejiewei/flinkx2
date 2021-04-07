/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.pg9wal;

import com.dtstack.flinkx.util.ClassUtil;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.TelnetUtil;

import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.core.ServerVersion;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.ReplicationSlotInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2019/12/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class PgWalUtil {

    private static final Logger LOG = LoggerFactory.getLogger(PgWalUtil.class);

    public static final String DRIVER = "org.postgresql.Driver";
    public static final String SLOT_PRE = "flinkx_";
//    public static final String PUBLICATION_NAME = "dtstack_flinkx";
    public static final String PUBLICATION_NAME = "provider1";

    public static final String QUERY_LEVEL = "SHOW wal_level;";
    public static final String QUERY_MAX_SLOT = "SHOW max_replication_slots;";
    public static final String QUERY_SLOT = "SELECT * FROM pg_replication_slots;";
    public static final String DROP_DEAD_SLOT = "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name like 'flinkx_%%' and active = false AND slot_name <> '%s';";
    public static final String QUERY_TABLE_REPLICA_IDENTITY = "SELECT relreplident FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid WHERE n.nspname='%s' and c.relname='%s';";
    public static final String UPDATE_REPLICA_IDENTITY = "ALTER TABLE %s REPLICA IDENTITY FULL;";
//    public static final String QUERY_PUBLICATION_NODE = "SELECT COUNT(1) FROM pglogical.node WHERE node_name = '%s';";
//    public static final String CREATE_PUBLICATION_NODE = "SELECT pglogical.create_node(node_name := '%s',dsn := 'host=localhost port=11085 dbname=postgres');";
    public static final String QUERY_PUBLICATION = "SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s';";
    public static final String CREATE_PUBLICATION = "CREATE PUBLICATION %s FOR ALL TABLES;";
    public static final String QUERY_TYPES = "SELECT t.oid AS oid, t.typname AS name FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A';";
    
    public static  Pg9RelicationSlot checkPostgres(PgConnection conn, boolean allowCreateSlot, String slotName, List<String> tableList) throws Exception{
        ResultSet resultSet;
        Pg9RelicationSlot availableSlot = null;

        //1. check postgres version
        // this Judge maybe not need?
        if (!conn.haveMinimumServerVersion(ServerVersion.v9_4)){
            String version = conn.getDBVersionNumber();
            LOG.error("postgres version must >= 9.4, current = [{}]", version);
            throw new UnsupportedOperationException("postgres version must >= 9.4, current = " + version);
        }
        
        //2. check postgres wal_level
        resultSet = conn.execSQLQuery(QUERY_LEVEL);
        resultSet.next();
        String wal_level = resultSet.getString(1);
        if(!"logical".equalsIgnoreCase(wal_level)){
            LOG.error("postgres wal_level must be logical, current = [{}]", wal_level);
            throw new UnsupportedOperationException("postgres wal_level must be logical, current = " + wal_level);
        }

        //3.check postgres slot
        resultSet = conn.execSQLQuery(QUERY_MAX_SLOT);
        resultSet.next();
        int maxSlot = resultSet.getInt(1);
        int slotCount = 0;
        resultSet = conn.execSQLQuery(QUERY_SLOT);
        while(resultSet.next()){
            Pg9RelicationSlot slot = new Pg9RelicationSlot();
            String name = resultSet.getString("slot_name");
            slot.setSlotName(name);
            slot.setActive(resultSet.getString("active"));

            if(name.equalsIgnoreCase(slotName) && slot.isNotActive()){
                slot.setPlugin(resultSet.getString("plugin"));
                slot.setSlotType(resultSet.getString("slot_type"));
                slot.setDatoid(resultSet.getInt("datoid"));
                slot.setDatabase(resultSet.getString("database"));
              //9.5版无此字段
//                slot.setTemporary(resultSet.getString("temporary"));
                //9.4版无此字段
//                slot.setActivePid(resultSet.getInt("active_pid"));
                slot.setXmin(resultSet.getString("xmin"));
                slot.setCatalogXmin(resultSet.getString("catalog_xmin"));
                slot.setRestartLsn(resultSet.getString("restart_lsn"));
              //9.5版无此字段
//                slot.setConfirmedFlushLsn(resultSet.getString("confirmed_flush_lsn"));
                availableSlot = slot;
                break;
            }
            if(!name.startsWith(SLOT_PRE) || StringUtils.equals("t",slot.getActive().toLowerCase())) {
            	slotCount++;
            }
        }

        //clear older slot
        String availableSlotName = "";
        if(availableSlot != null){
        	availableSlotName = availableSlot.getSlotName();
        }
        conn.createStatement().execute(String.format(DROP_DEAD_SLOT,availableSlotName));

        if(availableSlot == null){
        	slotCount++;
            if(!allowCreateSlot){
                String msg = String.format("there is no available slot named [%s], please check whether slotName[%s] is correct, or set allowCreateSlot = true", slotName, slotName);
                LOG.error(msg);
                throw new UnsupportedOperationException(msg);
            }else if(slotCount >= maxSlot){
                LOG.error("the number of slot reaches max_replication_slots[{}], please turn up max_replication_slots or remove unused slot", maxSlot);
                throw new UnsupportedOperationException("the number of slot reaches max_replication_slots[" + maxSlot + "], please turn up max_replication_slots or remove unused slot");
            }
        }

        //4.check table replica identity
        /*当原数据表发生更新时，默认的逻辑复制流只包含历史记录的key，如果需要输出更新记录的历史记录的所有字段，需要在表级别修改参数：
        REPLICA IDENTITY有四个值可选择：
        DEFAULT - 更新和删除将包含primary key列的现前值
        NOTHING - 更新和删除将不包含任何先前值
        FULL - 更新和删除将包含所有列的先前值
        INDEX index name - 更新和删除事件将包含名为index name的索引定义中包含的列的先前值。*/
        for (String table : tableList) {
            //schema.tableName
            String[] tables = table.split("\\.");
            resultSet = conn.execSQLQuery(String.format(QUERY_TABLE_REPLICA_IDENTITY, tables[0], tables[1]));
            if(resultSet.next()) {
	            String identity = parseReplicaIdentity(resultSet.getString(1));
	            if(!"full".equals(identity)){
	                LOG.warn("update {} replica identity, set {} to full", table, identity);
	                conn.createStatement().execute(String.format(UPDATE_REPLICA_IDENTITY, table));
	            }
            }
        }

        //5.check publication
//        resultSet = conn.execSQLQuery(String.format(QUERY_PUBLICATION, PUBLICATION_NAME));
//        resultSet.next();
//        long count = resultSet.getLong(1);
//        if(count == 0L){
//            LOG.warn("no publication named [{}] existed, flinkx will create one", PUBLICATION_NAME);
//            conn.createStatement().execute(String.format(CREATE_PUBLICATION, PUBLICATION_NAME));
//        }
        
//        resultSet = conn.execSQLQuery(String.format(QUERY_PUBLICATION_NODE, PUBLICATION_NAME));
//	    resultSet.next();
//	    long count = resultSet.getLong(1);
//	    if(count == 0L){
//	        LOG.warn("no publication named [{}] existed, flinkx will create one", PUBLICATION_NAME);
//	        conn.createStatement().execute(String.format(CREATE_PUBLICATION_NODE, PUBLICATION_NAME));
//	    }

        closeDBResources(resultSet, null, null, false);
        return availableSlot;
    }

    public static Pg9RelicationSlot createSlot(PGConnection conn, String slotName, boolean temporary) throws SQLException{
        /*ChainedLogicalCreateSlotBuilder builder = conn.getReplicationAPI()
                                                        .createReplicationSlot()
                                                        .logical()
                                                        .withSlotName(slotName)
                                                        .withOutputPlugin("pgoutput");*/
    	ReplicationSlotInfo slot1 = conn.getReplicationAPI()
							        .createReplicationSlot()
							        .logical()
							        .withSlotName(slotName)
//							        .withOutputPlugin("test_decoding")
							        .withOutputPlugin("ali_decoding")
							        .make();
//        if(temporary){
//            builder.withTemporaryOption();
//        }
//        ReplicationSlotInfo replicationSlotInfo = builder.make();
	      Pg9RelicationSlot slot = new Pg9RelicationSlot();
	      slot.setSlotName(slotName);
	      slot.setPlugin("ali_decoding");
//        slot.setSlotType(slot1.getReplicationType().toString());
//        slot.setTemporary(temporary);
//        slot.setConfirmedFlushLsn(replicationSlotInfo.getConsistentPoint().asString());
//        slot.setPlugin(replicationSlotInfo.getOutputPlugin());
        return slot;
    }

    public static Map<Integer, String> queryTypes(PgConnection conn) throws SQLException{
        Map<Integer, String> map = new HashMap<>(512);
        ResultSet resultSet = conn.execSQLQuery(QUERY_TYPES);
        while (resultSet.next()){
            int oid = (int) resultSet.getLong("oid");
            String typeName = resultSet.getString("name");
            map.put(oid, typeName);
        }
        closeDBResources(resultSet, null, conn, false);
        return map;
    }

    public static String parseReplicaIdentity(String s) {
        switch (s) {
            case "n":
                return "nothing";
            case "d":
                return "default";
            case "i" :
                return "index";
            case "f" :
                return "full";
            default:
                return "unknown";
        }
    }

    /**
     * 获取jdbc连接(超时10S)
     * @param url       url
     * @param username  账号
     * @param password  密码
     * @return
     * @throws SQLException
     */
    public static PGConnection getConnection(String url, String username, String password) throws SQLException {
        Connection dbConn;
        ClassUtil.forName(PgWalUtil.DRIVER, PgWalUtil.class.getClassLoader());
        Properties props = new Properties();
        PGProperty.USER.set(props, username);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        //postgres version must > 10
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);
            // telnet
            TelnetUtil.telnet(url);
            dbConn = DriverManager.getConnection(url, props);
        }

        return dbConn.unwrap(PGConnection.class);
    }
    
    public static PgConnection getSimpleConnection(String url, String username, String password) throws SQLException {
    	Connection dbConn;
    	ClassUtil.forName(PgWalUtil.DRIVER, PgWalUtil.class.getClassLoader());
        Properties props = new Properties();
         PGProperty.USER.set(props, username);
         PGProperty.PASSWORD.set(props, password);
         PGProperty.REPLICATION.set(props, "database");
         PGProperty.PREFER_QUERY_MODE.set(props, "simple");
         PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9");
    	synchronized (ClassUtil.LOCK_STR) {
            DriverManager.setLoginTimeout(10);
            // telnet
            TelnetUtil.telnet(url);
            dbConn = DriverManager.getConnection(url, props);
        }
    	 return dbConn.unwrap(PgConnection.class);
    }

    /**
     * 关闭连接资源
     *
     * @param rs     ResultSet
     * @param stmt   Statement
     * @param conn   Connection
     * @param commit
     */
    public static void closeDBResources(ResultSet rs, Statement stmt, Connection conn, boolean commit) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                LOG.warn("Close resultSet error: {}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                LOG.warn("Close statement error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }

        if (null != conn) {
            try {
                if (commit && !conn.isClosed()) {
                    conn.commit();
                }
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Close connection error:{}", ExceptionUtil.getErrorMessage(e));
            }
        }
    }

}
