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

import com.dtstack.flinkx.reader.MetaColumn;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Date: 2019/12/14
 * Company: www.dtstack.com
 *
 * reference to https://github.com/debezium/debezium & http://www.postgres.cn/docs/10/protocol-logicalrep-message-formats.html
 *
 * @author tudou
 */
public class Pg9Decoder {
    private static final Logger LOG = LoggerFactory.getLogger(Pg9Decoder.class);

    private static Instant PG_EPOCH = LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC);

    private volatile long currentLsn;
    private volatile long ts;

    public Pg9Decoder(Map<Integer, String> pgTypeMap) {
    }

    public Table decode(ByteBuffer buffer) throws SQLException {
        Table table = new Table();
        PgMessageTypeEnum type = PgMessageTypeEnum.forType((char) buffer.get());
        switch (type) {
            case BEGIN:
                //Byte1('B') ??????????????????????????????
                handleBeginMessage(buffer);
                System.out.println("HEX :" + Hex.encodeHexString(Arrays.copyOfRange(buffer.array(), 0, buffer.array().length)));
                break;
            case COMMIT:
                //Byte1('C') ??????????????????????????????
                handleCommitMessage(buffer);
                break;
            case INSERT:
                //Byte1('I') ??????????????????????????????
            case UPDATE:
                //Byte1('U') ??????????????????????????????
            case DELETE:
                //Byte1('D') ??????????????????????????????
                decodeTable(table,buffer);
            	break;
            default:
                break;
        }
        table.setType(type);
        return table;
    }

    private void handleBeginMessage(ByteBuffer buffer) {
    	int flag = buffer.getInt();

		long final_lsn = buffer.getLong();
		long commit_time = buffer.getLong();
		long xid = buffer.getInt();
    	
        //Int64 ??????????????????????????????PostgreSQL?????????2000-01-01??????????????????????????????
        Instant plus = PG_EPOCH.plus(commit_time, ChronoUnit.MICROS);
        currentLsn = final_lsn;
        ts = plus.toEpochMilli();
        LOG.trace("handleBeginMessage result = { lsn = {}, plus = {}, anInt = {}}", final_lsn, plus, xid);
    }

    private void handleCommitMessage(ByteBuffer buffer) {
        if(LOG.isTraceEnabled()){
        	int flag = buffer.getInt();
        	long commit_lsn = buffer.getLong();
        	long end_lsn = buffer.getLong();
        	long commit_time = buffer.getLong();
        	
            //Int64 ??????????????????????????????PostgreSQL?????????2000-01-01??????????????????????????????
            Instant commitTimestamp = PG_EPOCH.plus(commit_time, ChronoUnit.MICROS);
            LOG.trace("handleCommitMessage result = { flags = {}, lsn = {}, endLsn = {}, commitTimestamp = {}}", flag, commit_lsn, end_lsn, commitTimestamp);
        }
    }

    private void decodeTable(Table table, ByteBuffer buffer) {
    	table.setTs(ts);
    	table.setCurrentLsn(currentLsn);
    	decodeRelation( table,  buffer);
    	while(buffer.hasRemaining()) {
	    	PgAliMesTypeEnum type = PgAliMesTypeEnum.forType((char) buffer.get());
	    	Object object[] = null;
	    	switch (type) {
	            case COLUM:
	            	decodeColumn(table, buffer);
	            	break;
	            case NEWTUPLE:
	            	PgAliMesTypeEnum type1 = PgAliMesTypeEnum.forType((char) buffer.get());
	            	if(type1 == PgAliMesTypeEnum.TUPLE) {
		            	object = resolveColumnsFromStreamTupleData( buffer);
		            	table.setNewData(object);
	            	}
	            	break;
	            case OLDTUPLE:
	            	PgAliMesTypeEnum type2 = PgAliMesTypeEnum.forType((char) buffer.get());
	            	if(type2 == PgAliMesTypeEnum.TUPLE) {
		            	object = resolveColumnsFromStreamTupleData( buffer);
		            	table.setOldData(object);
	            	}
	            	break;
	            case KEYINFO:
	            	decodeKey(table, buffer);
	            	break;	
	            case NOKEY:
	            	LOG.debug("there is no primary key");
	            	break;	
	            case EMPTY:
	            	LOG.debug("delete is old value");
	            	object = new Object[table.getColumnList().size()];
	            	table.setOldData(object);
	            	break;	
	        }
    	}
    }

    
    private void decodeKey(Table table, ByteBuffer buffer)  {
    	short keyLength = buffer.getShort();
    	for(int i = 0 ; i < keyLength ; i++) {
    		short keyNameLength = buffer.getShort();
    		String name = readColumnValue2AsString(buffer,keyNameLength-1);
    		buffer.get();
    		LOG.debug("read key :{}",name);
    	}
    	
    }
    
    private void decodeRelation(Table table, ByteBuffer buffer)  {
    	short scheLength = buffer.getShort();
    	String schName = readColumnValue2AsString(buffer,scheLength-1);
    	table.setSchema(schName);
    	buffer.get();
    	
    	short tableLength = buffer.getShort();
    	String tableName = readColumnValue2AsString(buffer,tableLength-1);
    	table.setTable(tableName);
    	buffer.get();
    	
    	table.setId(schName+"."+tableName);
    }
    
    private void decodeColumn(Table table, ByteBuffer buffer)  {
    	short attrNum = buffer.getShort();
    	List<MetaColumn> columnList = new ArrayList<>();
    	for(int i = 0 ; i < attrNum ; i++) {
    		MetaColumn column = null;
    		short attrLength = buffer.getShort();
    		if(attrLength > 0) {
    			column = new MetaColumn();
    			column.setName(readColumnValue2AsString(buffer,attrLength-1));
    			buffer.get();
    			short attrNameLength = buffer.getShort();
    			column.setType(readColumnValue2AsString(buffer,attrNameLength-1));
    			buffer.get();
    		}
    		columnList.add(column);
    	}
    	table.setColumnList(columnList);
    }
    
    private Object[] resolveColumnsFromStreamTupleData(ByteBuffer buffer) {
        //Int16 ??????
        int numberOfColumns = buffer.getInt() ;
        Object[] data = new Object[numberOfColumns];
        for (int i = 0; i < numberOfColumns; i++) {

            //Byte1('n') ??????????????????NULL???
            //Byte1('u') ??????????????????TOASTed???????????????????????????
            //Byte1('t') ????????????????????????????????????
            char type = (char) buffer.get();
            if (type == 't') {
                data[i] = readColumnValue2AsString( buffer,buffer.getInt()-1) ;
                buffer.get();
            } else if (type == 'u') {
                data[i] = null;
            } else if (type == 'n') {
                data[i] = null;
            }
            
        }
        return data;
    }

    private static String readColumnValue2AsString(ByteBuffer buffer, int length) {
        //Int32 ???????????????
//        short length = buffer.getShort();
        byte[] value = new byte[length];
        //Byte(n) ???????????????????????????????????????n??????????????????
        buffer.get(value, 0, length);
        return new String(value);
    }

    private static String readString(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        byte b = 0;
        while ((b = buffer.get()) != 0) {
            sb.append((char) b);
        }
        return sb.toString();
    }



}
