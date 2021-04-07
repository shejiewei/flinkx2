package com.dtstack.flinkx.logminer.reader;

import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.logminer.Util.*;
import com.dtstack.flinkx.restore.FormatState;
import com.dtstack.flinkx.util.ExceptionUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;

import static com.dtstack.flinkx.logminer.Util.OracleConnectorSchema.*;


/**
 * Created by shejiewei on 2020/12/11.
 */
public class LogminerInputFormat extends BaseRichInputFormat {
    private static final Logger log = LoggerFactory.getLogger(LogminerInputFormat.class);
    protected String username;
    protected String password;
    protected String url;
    protected String databaseName;
    protected Integer interval;    //每次去拉取的间隔时间 单位秒
    protected boolean temporary;
    protected String id; //此id用于scn文件的生成路径

    protected boolean resetOffset;
    protected int fetchSize;  //logminer每次拉取的数目
    private Long streamOffsetScn;
    private Long streamOffsetCommitScn;
    private String streamOffsetRowId;

    private OracleSourceConnectorUtils utils;
    private static Connection dbConn;
    String logMinerOptions = OracleConnectorSQL.LOGMINER_START_OPTIONS;
    String logMinerOptionsDeSupportCM = OracleConnectorSQL.LOGMINER_START_OPTIONS_DESUPPORT_CM;
    String logMinerStartScr = OracleConnectorSQL.START_LOGMINER_CMD;
    CallableStatement logMinerStartStmt = null;
    CallableStatement logMinerStopStmt = null;
    String logMinerSelectSql;
    static PreparedStatement logMinerSelect;
    PreparedStatement currentSCNStmt;
    ResultSet logMinerData;
    ResultSet currentScnResultSet;
    private boolean closed = false;
    Boolean parseDmlData;
    static int ix = 0;
    boolean skipRecord = true;

    protected boolean pavingData = false;
    protected List<String> whiteList;
    protected List<String> blackList;
    protected String cat;
    Boolean oraDeSupportCM = true;
    // BlockingQueue<SourceRecord> sourceRecordMq = new LinkedBlockingQueue<>();
    LogMinerThread tLogMiner;
    private volatile long startLsn;
    protected Long scn;

    private transient BlockingQueue<Map<String, Object>> queue;
    private transient ExecutorService executor;
    private volatile boolean running = false;

    public String version() {
        return VersionUtil.getVersion();
    }

    public static Connection getThreadConnection() {
        return dbConn;
    }

    public static void closeDbConn() throws SQLException {
        logMinerSelect.cancel();
        dbConn.close();
    }
    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        //在这里初始化文件

        executor = Executors.newFixedThreadPool(1);
        queue = new SynchronousQueue<>(true);

    }
    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {
        if (inputSplit.getSplitNumber() != 0) {
            LOG.info("binlog openInternal split number:{} abort...", inputSplit.getSplitNumber());
            return;
        }

        LOG.info("oracle logminer openInternal split number:{} start...", inputSplit.getSplitNumber());

        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Long startSCN =scn;
        //log.info("Oracle Kafka Connector is starting on {}", config.getDbNameAlias());
        try {
            dbConn = DbUtil.getConnection(url, username, password);
            utils = new OracleSourceConnectorUtils(dbConn, whiteList,blackList);
            int dbVersion = utils.getDbVersion();
            log.info("Connected to database version {}", dbVersion);
            logMinerSelectSql = utils.getLogMinerSelectSql();

            log.info("Starting LogMiner Session");
            if (dbVersion >= ORA_DESUPPORT_CM_VERSION) {
                log.info("Db Version is {} and CONTINOUS_MINE is desupported", dbVersion);
                oraDeSupportCM = true;
                logMinerSelectSql = utils.getLogMinerSelectSqlDeSupportCM();
            }
            logMinerStartScr = logMinerStartScr + (oraDeSupportCM ? logMinerOptionsDeSupportCM : logMinerOptions) + ") \n; end;";
            //logMinerStartScr=logMinerStartScr+logMinerOptions+") \n; end;";
            logMinerStartStmt = dbConn.prepareCall(logMinerStartScr);   //开始执行logminer分析
            Map<String, Object> offset = null;// context.offsetStorageReader().offset(Collections.singletonMap(LOG_MINER_OFFSET_FIELD, dbName));
            Long localSCN = BackupUtil.getLocalJobBackup(id); //本地备份的scn位置,当程序重启是可以用
            if(localSCN!=null)
            {
                streamOffsetScn=localSCN;
            }
            else {
                streamOffsetScn = 0L;
            }
            streamOffsetCommitScn = 0L;
            streamOffsetRowId = "";
            if (offset != null) {
                Object lastRecordedOffset = offset.get(POSITION_FIELD);
                Object commitScnPositionObject = offset.get(COMMITSCN_POSITION_FIELD);
                Object rowIdPositionObject = offset.get(ROWID_POSITION_FIELD);
                streamOffsetScn = (lastRecordedOffset != null) ? Long.parseLong(String.valueOf(lastRecordedOffset)) : 0L;
                streamOffsetCommitScn = (commitScnPositionObject != null) ? Long.parseLong(String.valueOf(commitScnPositionObject)) : 0L;
                streamOffsetRowId = (rowIdPositionObject != null) ? (String) offset.get(ROWID_POSITION_FIELD) : "";
                if (oraDeSupportCM) streamOffsetScn = streamOffsetCommitScn;
                log.info("Offset values , scn:{},commitscn:{},rowid:{}", streamOffsetScn, streamOffsetCommitScn, streamOffsetRowId);
            }

            if (streamOffsetScn != 0L) {
                if (!oraDeSupportCM) {
                   // streamOffsetCtrl = streamOffsetScn;
                    PreparedStatement lastScnFirstPosPs = dbConn.prepareCall(OracleConnectorSQL.LASTSCN_STARTPOS);
                    lastScnFirstPosPs.setLong(1, streamOffsetScn);
                    lastScnFirstPosPs.setLong(2, streamOffsetScn);
                    ResultSet lastScnFirstPosRSet = lastScnFirstPosPs.executeQuery();
                    while (lastScnFirstPosRSet.next()) {
                        streamOffsetScn = lastScnFirstPosRSet.getLong("FIRST_CHANGE#");
                    }
                    lastScnFirstPosRSet.close();
                    lastScnFirstPosPs.close();
                    log.info("Captured last SCN has first position:{}", streamOffsetScn);
                }
            }

            if (!startSCN.equals("")&&startSCN!=0) {
               // log.info("Resetting offset with specified start SCN:{}", startSCN);
                 streamOffsetScn = startSCN;
                //streamOffsetScn-=1;
                //skipRecord = false;
            }

            if (resetOffset) {
                log.info("Resetting offset with new SCN");
                streamOffsetScn = 0L;
                streamOffsetCommitScn = 0L;
                streamOffsetRowId = "";
            }

            if (streamOffsetScn == 0L) {
                skipRecord = false;
                currentSCNStmt = dbConn.prepareCall(OracleConnectorSQL.CURRENT_DB_SCN_SQL);
                currentScnResultSet = currentSCNStmt.executeQuery();
                while (currentScnResultSet.next()) {
                    streamOffsetScn = currentScnResultSet.getLong("CURRENT_SCN");  //开始的scn
                }
                currentScnResultSet.close();
                currentSCNStmt.close();
                log.info("Getting current scn from database {}", streamOffsetScn);
            }

            log.info("Commit SCN : " + streamOffsetCommitScn);
            log.info(String.format("Log Miner will start at new position SCN : %s with fetch size : %s", streamOffsetScn, fetchSize));
            if (!oraDeSupportCM) {
                logMinerStartStmt.setLong(1, streamOffsetScn);
                logMinerStartStmt.execute();
                logMinerSelect = dbConn.prepareCall(logMinerSelectSql);
                logMinerSelect.setFetchSize(fetchSize);
                logMinerSelect.setLong(1, streamOffsetCommitScn);
                logMinerData = logMinerSelect.executeQuery();
                log.info("Logminer started successfully");
            } else {
                try {
                  /*  if(scn != 0){
                        startLsn = scn;
                    }else if(formatState != null && formatState.getState() != null){
                        startLsn = (long)formatState.getState();
                    }*/
                    executor.submit( new LogMinerThread(this,id,url,password,username, dbConn,streamOffsetScn, logMinerStartStmt, logMinerSelectSql, fetchSize, databaseName,interval));
                    running = true;
                }catch (Exception e){
                    LOG.error("PgWalInputFormat open() failed, e = {}", ExceptionUtil.getErrorMessage(e));
                    throw new RuntimeException("PgWalInputFormat open() failed, e = " + ExceptionUtil.getErrorMessage(e));
                }
                LOG.info("PgWalInputFormat[{}]open: end", jobName);

            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    @Override
    public FormatState getFormatState() {
        if (!restoreConfig.isRestore()) {
            LOG.info("return null for formatState");
            return null;
        }

        super.getFormatState();
        if (formatState != null) {
            formatState.setState(startLsn);
        }
        return formatState;
    }


    @Override
    protected Row nextRecordInternal(Row row) throws IOException {
        try {
            Map<String, Object> map=new HashedMap();
            try {
             map = queue.take();
            }catch (Exception e){
                System.out.println("e");
            }

                if (map.size() == 1) {
                    throw new IOException((String) map.get("e"));
                } else {
                    row = Row.of(map);
                }

        } catch (Exception e) {
            LOG.error("takeEvent interrupted error:{}", ExceptionUtil.getErrorMessage(e));
        }
        return row;
    }

    @Override
    protected void closeInternal() throws IOException {
        if (running) {
            executor.shutdownNow();
            running = false;
            LOG.warn("shutdown SqlServerCdcListener......");
        }
    }
    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        InputSplit[] splits = new InputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new GenericInputSplit(i, minNumSplits);
        }
        return splits;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }


    public void processEvent(Map<String, Object> event) {
        try {
            queue.put(event);
        } catch (Exception e) {
            LOG.error("takeEvent interrupted event:{} error:{}", event, ExceptionUtil.getErrorMessage(e));
        }
    }



    public boolean isPavingData() {
        return pavingData;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    public String getCat() {
        return cat;
    }

    public void setCat(String cat) {
        this.cat = cat;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public List<String> getWhiteList() {
        return whiteList;
    }

    public void setWhiteList(List<String> whiteList) {
        this.whiteList = whiteList;
    }

    public List<String> getBlackList() {
        return blackList;
    }

    public void setBlackList(List<String> blackList) {
        this.blackList = blackList;
    }
}
