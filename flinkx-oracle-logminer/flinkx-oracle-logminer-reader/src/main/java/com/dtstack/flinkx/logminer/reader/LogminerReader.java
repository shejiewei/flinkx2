/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.logminer.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.util.Md5Util;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;


/**
 * Created by shejiewei on 2020/12/11.
 */
public class LogminerReader extends BaseDataReader {

    private LogminerConfig binlogConfig;
    protected String username;
    protected String password;
    protected String url;
    protected String databaseName;


    protected Integer interval;

    protected String slotName;
    protected boolean allowCreateSlot;
    protected boolean temporary;


    protected boolean resetOffset;
    protected Long fetchSize;


    protected String cat;
    protected boolean pavingData;

    protected List<String> whiteList;
    protected List<String> blackList;
    protected Long scn;


    @SuppressWarnings("unchecked")
    public LogminerReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        username = readerConfig.getParameter().getStringVal(LogminerConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(LogminerConfigKeys.KEY_PASSWORD);
        url = readerConfig.getParameter().getStringVal(LogminerConfigKeys.KEY_URL);
        databaseName = readerConfig.getParameter().getStringVal(LogminerConfigKeys.KEY_DATABASE_NAME);
        cat = readerConfig.getParameter().getStringVal(LogminerConfigKeys.KEY_CATALOG);
        pavingData = readerConfig.getParameter().getBooleanVal(LogminerConfigKeys.KEY_PAVING_DATA, false);
        whiteList = (List<String>) readerConfig.getParameter().getVal(LogminerConfigKeys.KEY_WHITE_LIST);
        blackList = (List<String>) readerConfig.getParameter().getVal(LogminerConfigKeys.KEY_BLACK_LIST);
        interval = readerConfig.getParameter().getIntVal(LogminerConfigKeys.KEY_INTERVAL, 5);
        scn = readerConfig.getParameter().getLongVal(LogminerConfigKeys.KEY_SCN, 0);
        temporary = readerConfig.getParameter().getBooleanVal(LogminerConfigKeys.KEY_TEMPORARY, true);
    }

    @Override
    public DataStream<Row> readData() {
        LogminerInputFormatBuilder builder = new  LogminerInputFormatBuilder();

        String ID = Md5Util.getMd5(toString());
        builder.setID(ID);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setUrl(url);
        builder.setDatabaseName(databaseName);
        builder.setCat(cat);
        builder.setPavingData(pavingData);
        builder.setWhiteList(whiteList);
        builder.setBlackList(blackList);
        builder.setRestoreConfig(restoreConfig);
        builder.setStatusInterval(interval);
        builder.setScn(scn);
        builder.setTemporary(temporary);
        builder.setDataTransferConfig(dataTransferConfig);

       // builder.setRestoreConfig(restoreConfig);
        builder.setLogConfig(logConfig);
      //  builder.setTestConfig(testConfig);

        return createInput(builder.finish(), "oraclereader");
    }

    @Override
    public String toString() {
        return "LogminerReader{" +
                "binlogConfig=" + binlogConfig +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", url='" + url + '\'' +
                ", databaseName='" + databaseName + '\'' +
                ", interval=" + interval +
                ", slotName='" + slotName + '\'' +
                ", allowCreateSlot=" + allowCreateSlot +
                ", temporary=" + temporary +
                ", resetOffset=" + resetOffset +
                ", fetchSize=" + fetchSize +
                ", cat='" + cat + '\'' +
                ", pavingData=" + pavingData +
                ", whiteList=" + whiteList +
                ", blackList=" + blackList +
                ", scn=" + scn +
                '}';
    }
}
