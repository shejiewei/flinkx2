/*
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
package com.dtstack.flinkx.kafka.reader;

import com.dtstack.flinkx.decoder.IDecode;
import com.dtstack.flinkx.kafkabase.reader.IClient;
import com.dtstack.flinkx.kafkabase.reader.KafkaBaseInputFormat;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2019/12/25
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class KafkaClient implements IClient {
    protected static Logger LOG = LoggerFactory.getLogger(KafkaClient.class);
    private volatile boolean running = true;
    private long pollTimeout;
    private boolean blankIgnore;
    private IDecode decode;
    private KafkaBaseInputFormat format;
    private KafkaConsumer<String, String> consumer;
    //用来记录当前消费的偏移
    private static Map<TopicPartition, Long> offsets = new HashMap<>();

    //用来记录当需要提交的偏移
    private static Map<TopicPartition, OffsetAndMetadata> commitOffset = new HashMap<>();

    public KafkaClient(Properties clientProps, List<String> topics, long pollTimeout, KafkaBaseInputFormat format) {
        this.pollTimeout = pollTimeout;
        this.blankIgnore = format.getBlankIgnore();
        this.format = format;
        this.decode = format.getDecode();
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(clientProps);
        consumer.subscribe(topics);
    }

   @Override
    public void run() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, String> r : records) {
                    boolean isIgnoreCurrent = r.value() == null || blankIgnore && StringUtils.isBlank(r.value());
                    if (isIgnoreCurrent) {
                        continue;
                    }
                    
                    try {
                        processMessage(r.value());

                    } catch (Throwable e) {
                        LOG.error("kafka consumer fetch is error, message:{}, e = {}", r.value(), ExceptionUtil.getErrorMessage(e));
                    }
                }

                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    // 获取当前读取到的最后一条记录的offset
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    // 提交offset
                    offsets.put(partition, lastOffset + 1);
                }

                //
                for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                    commitOffset.put(entry.getKey(), new OffsetAndMetadata(offsets.get(entry.getKey())));
                    LOG.warn("partition[{}], 当前待提交kafka偏移:[{}]", entry.getKey().partition(), offsets.get(entry.getKey()));
                }
                 // 异步提交offset
                consumer.commitAsync(commitOffset, (offsets, exception) -> {
                    if (exception != null) {
                        LOG.error("fail to commit offsets {}, {}", offsets, exception);
                        // 同步提交，来做offset提交最后的保证。
                        consumer.commitSync();
                    }
                });

            }
        } catch (WakeupException e) {
            LOG.warn("WakeupException to close kafka consumer, e = {}", ExceptionUtil.getErrorMessage(e));
        } catch (Throwable e) {
            LOG.error("kafka consumer fetch is error, e = {}", ExceptionUtil.getErrorMessage(e));
        } finally {
            consumer.close();
        }
    }


  /*  @Override
    public void run() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                for (ConsumerRecord<String, String> r : records) {
                    boolean isIgnoreCurrent = r.value() == null || blankIgnore && StringUtils.isBlank(r.value());
                    if (isIgnoreCurrent) {
                        continue;
                    }

                    try {
                        processMessage(r.value());
                    } catch (Throwable e) {
                        LOG.error("kafka consumer fetch is error, message:{}, e = {}", r.value(), ExceptionUtil.getErrorMessage(e));
                    }
                }
            }
        } catch (WakeupException e) {
            LOG.warn("WakeupException to close kafka consumer, e = {}", ExceptionUtil.getErrorMessage(e));
        } catch (Throwable e) {
            LOG.error("kafka consumer fetch is error, e = {}", ExceptionUtil.getErrorMessage(e));
        } finally {
            consumer.close();
        }
    }*/
    @Override
    public void processMessage(String message) {
        Map<String, Object> event = decode.decode(message);
        if (event != null && event.size() > 0) {
            format.processEvent(event);
        }
    }

    @Override
    public void close() {
        try {
            running = false;
            consumer.wakeup();
        } catch (Exception e) {
            LOG.error("close kafka consumer error, e = {}", ExceptionUtil.getErrorMessage(e));
        }
    }

}
