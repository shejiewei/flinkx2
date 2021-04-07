package com.dtstack.flinkx.test;/*
package com.dtstack.flinkx.test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dtstack.flink.api.java.MyLocalStreamEnvironment;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.SpeedConfig;
import com.dtstack.flinkx.constants.ConfigConstant;
import com.dtstack.flinkx.pg9wal.reader.Pg9walReader;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.stream.writer.StreamWriter;
import com.dtstack.flinkx.writer.BaseDataWriter;

public class TestPg9 {
	
	private static final int FAILURE_RATE = 3;
    private static final int FAILURE_INTERVAL = 6;
    private static final int DELAY_INTERVAL = 10;
    public static Logger LOG = LoggerFactory.getLogger(TestPg9.class);
    public static Configuration conf = new Configuration();


	public static void main(String[] args) {
		System.out.println("asd");
	}
	

	public static JobExecutionResult runJob(File jobFile, Properties confProperties, String savepointPath) throws Exception{
        String jobContent = readJob(jobFile);
        return runJob(jobContent, confProperties, savepointPath);
    }

    public static JobExecutionResult runJob(String job, Properties confProperties, String savepointPath) throws Exception{
        DataTransferConfig config = DataTransferConfig.parse(job);

        conf.setString("akka.ask.timeout", "180 s");
        conf.setString("web.timeout", String.valueOf(100000));

        MyLocalStreamEnvironment env = new MyLocalStreamEnvironment(conf);

        openCheckpointConf(env, confProperties);

        env.setParallelism(config.getJob().getSetting().getSpeed().getChannel());

        if (needRestart(config)) {
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    10,
                    Time.of(10, TimeUnit.SECONDS)
            ));
        }

        BaseDataReader reader = buildDataReader(config, env);
        DataStream<Row> dataStream = reader.readData();
        SpeedConfig speedConfig = config.getJob().getSetting().getSpeed();
        if (speedConfig.getReaderChannel() > 0) {
            dataStream = ((DataStreamSource<Row>) dataStream).setParallelism(speedConfig.getReaderChannel());
        }

        if (speedConfig.isRebalance()) {
            dataStream = dataStream.rebalance();
        }

        BaseDataWriter dataWriter = buildDataWriter(config);
        DataStreamSink<?> dataStreamSink = dataWriter.writeData(dataStream);
        if (speedConfig.getWriterChannel() > 0) {
            dataStreamSink.setParallelism(speedConfig.getWriterChannel());
        }

        if(StringUtils.isNotEmpty(savepointPath)){
            env.setSettings(SavepointRestoreSettings.forPath(savepointPath));
        }

        return env.execute();
    }

    private static boolean needRestart(DataTransferConfig config){
        return config.getJob().getSetting().getRestoreConfig().isRestore();
    }

    private static String readJob(File file) {
        try(FileInputStream in = new FileInputStream(file)) {
            byte[] fileContent = new byte[(int) file.length()];
            in.read(fileContent);
            return new String(fileContent, StandardCharsets.UTF_8);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private static BaseDataReader buildDataReader(DataTransferConfig config, StreamExecutionEnvironment env){
        String readerName = config.getJob().getContent().get(0).getReader().getName();
        BaseDataReader reader ;
        switch (readerName){
	      //  case PluginNameConstants.PGWAL_READER : reader = new Pg9walReader(config, env); break;
//		        case PluginNameConstants.BINLOG_READER : reader = new BinlogReader(config, env); break;
            default:throw new IllegalArgumentException("Can not find reader by name:" + readerName);
        }

        return reader;
    }

    private static BaseDataWriter buildDataWriter(DataTransferConfig config){
        String writerName = config.getJob().getContent().get(0).getWriter().getName();
        BaseDataWriter writer;
        switch (writerName){
            case PluginNameConstants.STREAM_WRITER : writer = new StreamWriter(config); break;
            default:throw new IllegalArgumentException("Can not find writer by name:" + writerName);
        }

        return writer;
    }

    private static void openCheckpointConf(StreamExecutionEnvironment env, Properties properties){
        if(properties == null){
            return;
        }

        if(properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY) == null){
            return;
        }else{
            long interval = Long.parseLong(properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_INTERVAL_KEY).trim());

            //start checkpoint every ${interval}
            env.enableCheckpointing(interval);

            LOG.info("Open checkpoint with interval:" + interval);
        }

        String checkpointTimeoutStr = properties.getProperty(ConfigConstant.FLINK_CHECKPOINT_TIMEOUT_KEY);
        if(checkpointTimeoutStr != null){
            long checkpointTimeout = Long.parseLong(checkpointTimeoutStr);
            //checkpoints have to complete within one min,or are discard
            env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);

            LOG.info("Set checkpoint timeout:" + checkpointTimeout);
        }

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setStateBackend(new FsStateBackend(new Path("file:///tmp/flinkx_checkpoint")));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                FAILURE_RATE,
                Time.of(FAILURE_INTERVAL, TimeUnit.MINUTES),
                Time.of(DELAY_INTERVAL, TimeUnit.SECONDS)
        ));
    }
}
*/
