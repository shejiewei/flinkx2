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

package com.dtstack.flinkx.pg9wal.listener;

import com.dtstack.flinkx.pg9wal.Pg9Decoder;
import com.dtstack.flinkx.pg9wal.PgDecoder;
import com.dtstack.flinkx.pg9wal.PgWalUtil;
import com.dtstack.flinkx.pg9wal.Table;
import com.dtstack.flinkx.pg9wal.format.Pg9WalInputFormat;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.dtstack.flinkx.util.JsonModifyUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import scala.util.parsing.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.postgresql.PGConnection;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Date: 2019/12/14
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class Pg9WalListener implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(Pg9WalListener.class);
    private static Gson gson = new Gson();

    private Pg9WalInputFormat format;
    private PGConnection conn;
    private PgConnection reconn;
    private Set<String> tableSet;
    private Set<String> cat;
    private boolean pavingData;

    private PGReplicationStream stream;
    private Pg9Decoder decoder;

    public Pg9WalListener(Pg9WalInputFormat format) {
        this.format = format;
        this.conn = format.getConn();
        this.reconn = format.getReconn();
        this.tableSet = new HashSet<>(format.getTableList());
        this.cat = new HashSet<>();
        for (String type : format.getCat().split(",")) {
            cat.add(type.toLowerCase());
        }
        this.pavingData = format.isPavingData();
    }

   /* public void init() throws Exception{
        decoder = new PgDecoder(PgWalUtil.queryTypes(conn));
        ChainedLogicalStreamBuilder builder = conn.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(format.getSlotName())
                .withSlotOption("include-xids", false)
                .withSlotOption("skip-empty-xacts", true)
                //协议版本。当前仅支持版本1
//                .withSlotOption("proto_version", "1")
                //逗号分隔的要订阅的发布名称列表（接收更改）。 单个发布名称被视为标准对象名称，并可根据需要引用
//                .withSlotOption("publication_names", PgWalUtil.PUBLICATION_NAME)
                .withStatusInterval(format.getStatusInterval(), TimeUnit.MILLISECONDS);
        long lsn = format.getStartLsn();
        if(lsn != 0){
            builder.withStartPosition(LogSequenceNumber.valueOf(lsn));
        }
        stream = builder.start();
        TimeUnit.SECONDS.sleep(1);
        stream.forceUpdateStatus();
        LOG.info("init PGReplicationStream successfully...");
    }*/
    
    public void init() throws Exception{
        decoder = new Pg9Decoder(PgWalUtil.queryTypes(reconn));
        ChainedLogicalStreamBuilder builder = conn.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(format.getSlotName())
//                .withSlotOption("include-xids", false)
//                .withSlotOption("skip-empty-xacts", true)
                //协议版本。当前仅支持版本1
//                .withSlotOption("proto_version", "1")
                //逗号分隔的要订阅的发布名称列表（接收更改）。 单个发布名称被视为标准对象名称，并可根据需要引用
//                .withSlotOption("publication_names", PgWalUtil.PUBLICATION_NAME)
                .withStatusInterval(format.getStatusInterval(), TimeUnit.MILLISECONDS);
        long lsn = format.getStartLsn();
        if(lsn != 0){
            builder.withStartPosition(LogSequenceNumber.valueOf(lsn));
        }
        stream = builder.start();
        if(lsn == 0) {
        	LOG.info("LastReceiveLSN："+stream.getLastReceiveLSN());
        	stream.setAppliedLSN(stream.getLastReceiveLSN());
        	stream.setFlushedLSN(stream.getLastReceiveLSN());
        }
        TimeUnit.SECONDS.sleep(1);
        stream.forceUpdateStatus();
        LOG.info("init PGReplicationStream successfully...");
    }


    @Override
    public void run() {
        LOG.info("PgWalListener start running.....");
        try {
            init();
            while (format.isRunning()) {
                ByteBuffer buffer = stream.readPending();
                if (buffer == null) {
                    continue;
                }
                
                stream.setAppliedLSN(stream.getLastReceiveLSN());
                stream.setFlushedLSN(stream.getLastReceiveLSN());
                
                Table table = decoder.decode(buffer);
                if(StringUtils.isBlank(table.getId())){
                    continue;
                }
                String type = table.getType().name().toLowerCase();
                if(!cat.contains(type)){
                    continue;
                }
                if(!tableSet.contains(table.getId())){
                    continue;
                }
//                LOG.info("table = {}",gson.toJson(table));
                Map<String, Object> map = new LinkedHashMap<>();
                map.put("type", type);
                map.put("schema", table.getSchema());
                map.put("table", table.getTable());
                map.put("lsn", table.getCurrentLsn());
                map.put("ts", table.getTs());
                map.put("ingestion", System.nanoTime());
                table.setOldData(table.getOldData() == null ? new Object[table.getColumnList().size()] : table.getOldData());
                table.setNewData(table.getNewData() == null ? new Object[table.getColumnList().size()] : table.getNewData());
                if(pavingData){
                    int i = 0;
                    for (MetaColumn column : table.getColumnList()) {
                    	if(column != null) {
	                        map.put("before_" + column.getName(), table.getOldData()[i]);
	                        map.put("after_" + column.getName(), table.getNewData()[i]);
                    	}
                    	i++;
                    }
                }else {
                    Map<String, Object> before = new LinkedHashMap<>();
                    Map<String, Object> after = new LinkedHashMap<>();
                    int i = 0;
                    for (MetaColumn column : table.getColumnList()) {
                    	if(column != null) {
	                        before.put(column.getName(), table.getOldData()[i]);
	                        after.put(column.getName(), table.getNewData()[i]);
                    	}
                    	i++;
                    }
                    map.put("before", before);
                    map.put("after", after);
                }
                LOG.info("data:{}",gson.toJson(map));
                format.processEvent(map);
            }
        }catch (Exception e){
            String errorMessage = ExceptionUtil.getErrorMessage(e);
            LOG.error(errorMessage);
            format.processEvent(Collections.singletonMap("e", errorMessage));

        }
    }
}
