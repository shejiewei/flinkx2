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

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;


public class LogminerWriter extends BaseDataWriter {


    private String url;
    private String username;
    private String password;
    private int batchSize;
    private int delayTime;
    public LogminerWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        url = writerConfig.getParameter().getStringVal(LogminerConfigKeys.KEY_URL);
        username = writerConfig.getParameter().getStringVal(LogminerConfigKeys.KEY_USER_NAME);
        password = writerConfig.getParameter().getStringVal(LogminerConfigKeys.KEY_PASSWORD);
        batchSize= writerConfig.getParameter().getIntVal(LogminerConfigKeys.KEY_BATCH_SIZE,1);
        delayTime= writerConfig.getParameter().getIntVal(LogminerConfigKeys.KEY_DELAY_TIME,30);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        LogminerOutputFormatBuilder builder = new LogminerOutputFormatBuilder();
        builder.setUrl(url);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setMonitorUrls(monitorUrls);
        builder.setDelayTime(delayTime);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);

        builder.setBatchInterval(batchSize);

        builder.setRestoreConfig(restoreConfig);
        return createOutput(dataSet, builder.finish());
    }
}
