package com.dtstack.flinkx.logminer.Util;

import com.dtstack.flinkx.logminer.Util.models.DMLRow;
import com.dtstack.flinkx.logminer.Util.models.Transaction;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.google.gson.Gson;
import com.dtstack.flinkx.logminer.reader.LogminerInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.sql.*;
import java.util.*;

import static com.dtstack.flinkx.logminer.Util.OracleConnectorSchema.*;


public class LogMinerThread implements Runnable {
    static final Logger log = LoggerFactory.getLogger(LogMinerThread.class);
    //private BlockingQueue<SourceRecord> sourceRecordMq;

    private Connection dbConn;
    private String url;
    private String username;
    private String password;
    private Long streamOffsetScn;
    private Long streamOffsetScnBackup;
    private Long streamOffsetCtrl;
    private Long streamOffsetCommitScn;
    private String streamOffsetOperation;
    private String streamOffsetRowId;
    private String streamOffsetXid;
    private String id; //此id用于scn文件的生成路径

    CallableStatement logMinerStartStmt;
    PreparedStatement logMinerSelect;
    String logMinerSelectSql;
    int dbFetchSize;
    ResultSet logMinerData;
    private boolean closed = false;
    Transaction transaction = null;
    LinkedHashMap<String, Transaction> trnCollection = new LinkedHashMap<>();
    boolean skipRecord = true;
    static int ix = 0;
    String sqlX = "";
    int sequence = 0;
    int oldSequence = 0;

    private String dbNameAlias = null;


    private static Gson gson = new Gson();

    private LogminerInputFormat format;

    private Set<String> tableSet;

    private boolean pavingData;
    String logMinerStartScr = OracleConnectorSQL.START_LOGMINER_CMD;
    Boolean oraDeSupportCM = true;
    String logMinerOptionsDeSupportCM = OracleConnectorSQL.LOGMINER_START_OPTIONS_DESUPPORT_CM;
    String logMinerOptions = OracleConnectorSQL.LOGMINER_START_OPTIONS;
    private int interval;

    public LogMinerThread(LogminerInputFormat format,String id, String url, String password, String username, Connection dbConn, Long streamOffsetScn, CallableStatement logMinerStartStmt, String logMinerSelectSql, int dbFetchSize, String dbNameAlias, int interval) {
        this.format = format;
        this.id=id;
        this.tableSet = new HashSet<>(format.getWhiteList());
        this.pavingData = format.isPavingData();
        this.dbConn = dbConn;
        this.streamOffsetScn = streamOffsetScn;
        this.streamOffsetScnBackup = streamOffsetScn;
        this.logMinerStartStmt = logMinerStartStmt;
        this.logMinerSelectSql = logMinerSelectSql;
        this.dbFetchSize = dbFetchSize;
        this.dbNameAlias = dbNameAlias;
        this.url = url;
        this.password = password;
        this.username = username;
        this.interval = interval;
        //  this.utils = utils;
    }

    @Override
    public void run() {
        try {
            while (!this.closed) {
             //   log.info("Log miner process is waiting for 5 seconds before start on Thread");
                Thread.sleep(interval * 1000);
                skipRecord = false;
                int iError = 0;

                while (true) {
                    Boolean newLogFilesExists = false;
                    try {
                        newLogFilesExists = OracleSqlUtils.getLogFilesV2(dbConn, streamOffsetScn);
                    } catch (Exception e) {
                        log.warn("网络断了,{}", e.toString());
                    }

                   // log.info("Log miner will start , startScn : {} ", streamOffsetScn);
                    try {
                        if (newLogFilesExists) {
                            logMinerStartStmt.setLong(1, streamOffsetScn);
                            logMinerStartStmt.execute();
                        }
                        logMinerSelect = dbConn.prepareCall(logMinerSelectSql);
                        logMinerSelect.setFetchSize(dbFetchSize);
                        logMinerSelect.setLong(1, streamOffsetScn);
                        logMinerData = logMinerSelect.executeQuery();
                    } catch (SQLException se) {
                        iError++;
                       /* if (iError > 100) {
                            log.error("Logminer could not start successfully and it will exit");
                            return;            //这里注释掉,不允许关闭线程
                        }  */
                        //让connection重连
                        log.error("Logminer start exception {},{}秒后,重连第{}次 ", se.getMessage(), 2 * iError, iError);
                        try {
                            dbConn = new OracleConnection().connect(url, username, password);
                            String logMinerStart = logMinerStartScr + (oraDeSupportCM ? logMinerOptionsDeSupportCM : logMinerOptions) + ") \n; end;";
                            logMinerStartStmt = dbConn.prepareCall(logMinerStart);   //开始执行logminer分析 ,断网后要重新打开logminer分析
                            log.warn("重连成功");
                        } catch (Exception e) {
                            log.info("重连失败,{}", e);
                        }

                        log.info("Waiting for log switch");
                        Thread.sleep(2 * iError * 1000);

                        continue;
                    }
                    break;
                }
                //log.info("Logminer started successfully on Thread");
                while (!this.closed && logMinerData.next()) {
                    try {
                        if ((sequence > 0) && (logMinerData.getInt("RBASQN") - sequence) > 1) {
                            log.error("Captured archive and log files have changed , regetting log files");
                            break;
                        }
                        sequence = logMinerData.getInt("RBASQN");
                        String operation = logMinerData.getString(OPERATION_FIELD);
                        String xid = logMinerData.getString(XID_FIELD);
                        Long scn = logMinerData.getLong(SCN_FIELD);
                        Timestamp timeStamp = logMinerData.getTimestamp(TIMESTAMP_FIELD);
                        //  Timestamp commitTimeStamp=logMinerData.getTimestamp(COMMIT_TIMESTAMP_FIELD);
                        Long commitScn = logMinerData.getLong(COMMIT_SCN_FIELD);
                        String rowId = logMinerData.getString(ROW_ID_FIELD);
                        //#log.info(operation+"-"+xid+"-"+scn);

                        if (operation.equals(OPERATION_COMMIT)) {
                            transaction = trnCollection.get(xid);
                            if (transaction != null) {
                                //###log.info("Commit found for xid:{}",xid);
                                //transaction.setIsCompleted(true);
                                if (transaction.getContainsRollback()) {
                                    int deletedRows = 0;
                                    List<DMLRow> dmlRowCollOrigin = transaction.getDmlRowCollection();

                                    LinkedList<Integer> deleteList = new LinkedList<>();
                                    for (int r = 0; r < dmlRowCollOrigin.size(); r++) {
                                        //log.info(dmlRowCollOrigin.get(r).toString()+"-"+r);
                                        if (dmlRowCollOrigin.get(r).getRollback().equals("1")) {
                                            //log.info(dmlRowCollOrigin.get(r).toString()+"RB Work RBRBRB"+" - "+r+" - "+deletedRows);
                                            //log.info("Will delete "+(r-deletedRows)+":"+(r-1-deletedRows));
                                            deleteList.add(r - deletedRows);
                                            deleteList.add(r - 1 - deletedRows);
                                            deletedRows += 2;
                                        }
                                    }
                                    for (int it : deleteList) {
                                        //log.info("WILL DELETE "+it+"-"+dmlRowCollOrigin.get(it).getXid()+"-"+dmlRowCollOrigin.get(it).getSegName()+"-"+dmlRowCollOrigin.get(it).getRowId()+"-"+dmlRowCollOrigin.get(it).getOperation());
                                        dmlRowCollOrigin.remove(it);
                                    }

                                    //log.info("Setting rowCollection");
                                    transaction.setDmlRowCollection(dmlRowCollOrigin);
                                }
                                ListIterator<DMLRow> iterator = transaction.getDmlRowCollection().listIterator();
                                while (iterator.hasNext()) {
                                    //records.add(createRecords(iterator.next()));
                                    DMLRow row = iterator.next();
                                    // row.setCommitTimestamp(commitTimeStamp);
                                    row.setCommitScn(commitScn);
                                    ix++;
                                    if (ix % 10000 == 0)
                                        log.info(String.format("Fetched %s rows from db:%s ", ix, dbNameAlias) + " " + sequence + " " + oldSequence + " " + row.getScn() + " " + row.getCommitScn() + " " + row.getCommitTimestamp());
                                    //log.info(row.getScn()+"-"+row.getCommitScn()+"-"+row.getTimestamp()+"-"+"-"+row.getCommitTimestamp()+"-"+row.getXid()+"-"+row.getSegName()+"-"+row.getRowId()+"-"+row.getOperation());
                                    try {
                                        // sourceRecordMq.offer(createRecords(row));
                                    } catch (Exception eCreateRecord) {
                                        log.error("Error during create record on  xid :{} SQL :{}", xid, row.getSqlRedo(), eCreateRecord);
                                        continue;
                                    }
                                }
                                trnCollection.remove(xid);
                            }
                        }

                        if (operation.equals(OPERATION_ROLLBACK)) {
                            transaction = trnCollection.get(xid);
                            if (transaction != null) {
                                trnCollection.remove(xid);
                            }
                        }

                        if (operation.equals(OPERATION_START)) {
                            List<DMLRow> dmlRowCollectionNew = new ArrayList<>();
                            transaction = new Transaction(xid, scn, timeStamp, dmlRowCollectionNew, false);
                            trnCollection.put(xid, transaction);
                        }

                        if ((operation.equals(OPERATION_INSERT)) || (operation.equals(OPERATION_UPDATE)) || (operation.equals(OPERATION_DELETE)) || (operation.equals(OPERATION_DDL))) {

                            boolean contSF = logMinerData.getBoolean(CSF_FIELD);
                            String sqlRedo1 = logMinerData.getString(SQL_REDO_FIELD);
                            // String rollback=logMinerData.getString(ROLLBACK_FIELD);
                            Boolean trContainsRollback = false;
                            //rollback.equals("1") ? true : false;
                            if (skipRecord) {
                                if ((scn.equals(streamOffsetCtrl)) && (commitScn.equals(streamOffsetCommitScn)) && (rowId.equals(streamOffsetRowId)) && (!contSF)) {
                                    skipRecord = false;
                                }
                               // log.info("Skipping data with scn :{} Commit Scn :{} Rowid :{}", scn, commitScn, rowId);
                                continue;
                            }

                            String segOwner = logMinerData.getString(SEG_OWNER_FIELD);
                            String segName = logMinerData.getString(TABLE_NAME_FIELD);
                            String sqlRedo = logMinerData.getString(SQL_REDO_FIELD).replace("  ", "");
                            if (sqlRedo.contains(TEMPORARY_TABLE)) continue;
                            if (operation.equals(OPERATION_DDL) && (logMinerData.getString("INFO").startsWith("INTERNAL DDL")))
                                continue;
                            while (contSF) {
                                logMinerData.next();
                                sqlRedo += logMinerData.getString(SQL_REDO_FIELD);
                                contSF = logMinerData.getBoolean(CSF_FIELD);
                            }
                            sqlX = sqlRedo;
                            //log.info("redo:" + sqlRedo);
                            //log.info("commitScn:" + commitScn);

                            //@Data row = new Data(scn, segOwner, segName, sqlRedo,timeStamp,operation);
                            //@topic = config.getTopic().equals("") ? (config.getDbNameAlias()+DOT+row.getSegOwner()+DOT+row.getSegName()).toUpperCase() : topic;
                            // topicName = topicConfig.equals("") ? (dbNameAlias + DOT + segOwner + DOT + (operation.equals(OPERATION_DDL) ? DDL_TOPIC_POSTFIX : segName)).toUpperCase() : topicConfig;
                            DMLRow dmlRow = new DMLRow(xid, scn, commitScn, timeStamp, operation, segOwner, segName, rowId, sqlRedo, null, null);
                            //#log.info("Row :{} , scn:{} , commitScn:{} ,sqlRedo:{}",ix,scn,commitScn,sqlX);

                            //dmlRowCollection2.clear();
                            List<DMLRow> dmlRowCollection = new ArrayList<>();
                            //###log.info("txnCollection size:{}",trnCollection.size());
                            //DMLRow dmlRow = new DMLRow(xid, scn, timeStamp, operation, segOwner, segName, rowId, sqlRedo,topic);
                            transaction = trnCollection.get(xid);

                          //  log.info("LogminerListener start running.....");
                            try {
                                Map<String, Object> map = new LinkedHashMap<>();
                                map.put(SCN_FIELD, dmlRow.getScn());
                                map.put(SEG_OWNER_FIELD, dmlRow.getSegOwner());
                                map.put(TABLE_NAME_FIELD, dmlRow.getSegName());
                                map.put(TIMESTAMP_FIELD, dmlRow.getTimestamp());
                                map.put(SQL_REDO_FIELD, dmlRow.getSqlRedo());
                                map.put(OPERATION_FIELD, dmlRow.getOperation());
                                log.warn("解析出来的消息:" + map.toString());
                                try {
                                    format.processEvent(map);
                                } catch (Exception e) {
                                    log.error(e.toString());
                                }

                            } catch (Exception e) {
                                String errorMessage = ExceptionUtil.getErrorMessage(e);
                                log.error(errorMessage);
                                format.processEvent(Collections.singletonMap("e", errorMessage));

                            }

                            if (transaction != null) {
                                dmlRowCollection = transaction.getDmlRowCollection();
                                dmlRowCollection.add(dmlRow);
                                transaction.setDmlRowCollection(dmlRowCollection);
                                if (!transaction.getContainsRollback())
                                    transaction.setContainsRollback(trContainsRollback);
                                //trnCollection.replace(xid, transaction);
                            } else {
                                //#log.error("Null Transaction {}",xid);
                                dmlRowCollection.add(dmlRow);
                                transaction = new Transaction(xid, scn, timeStamp, dmlRowCollection, trContainsRollback);
                                trnCollection.put(xid, transaction);
                            }
                        }
                        streamOffsetScn = scn + 1;  //这里加1,不然会出现重复打印
                        streamOffsetOperation = operation;
                        streamOffsetCommitScn = commitScn;
                        streamOffsetRowId = rowId;
                        streamOffsetXid = xid;
                        oldSequence = sequence;
                    } catch (Exception e) {
                        log.error("Inner Error during poll on  SQL :{}", sqlX, e);
                        continue;
                    }
                }

                //将scn保存到文件中    streamOffsetScnBackup来保存状态,如果streamOffsetScnBackup!=streamOffsetScn,说明发生了scn修改,就备份scn
                if (streamOffsetScnBackup != streamOffsetScn) {
                    BackupUtil.updateLocalBackup(id,streamOffsetScn);
                    streamOffsetScnBackup = streamOffsetScn;
                }

                logMinerData.close();
                logMinerSelect.close();
               // log.info("Logminer stopped successfully on Thread , scn:{},commitScn:{},operation:{},xid:{},rowid:{}", streamOffsetScn, streamOffsetCommitScn, streamOffsetOperation, streamOffsetXid, streamOffsetRowId);
               // log.info(trnCollection.toString());
            }
        } catch (InterruptedException ie) {
            log.error("Thread interrupted exception");
        } catch (RuntimeException re) {
            log.error("Thread runtime exception");
        } catch (Exception e) {
            log.error("Thread general exception {}", e);
            try {
                //OracleSqlUtils.executeCallableStmt(dbConn, OracleConnectorSQL.STOP_LOGMINER_CMD);   不关
                throw new ConnectException("Logminer stopped because of " + e.getMessage());
            } catch (Exception e2) {
                log.error("Thread general exception stop logminer {}", e2.getMessage());
            }
        }
    }

    public void shutDown() {
        log.info("Logminer Thread shutdown called");
        this.closed = true;
    }


    private Map<String, String> sourcePartition() {
        return Collections.singletonMap(LOG_MINER_OFFSET_FIELD, dbNameAlias);
    }

    private Map<String, String> sourceOffset(Long scnPosition, Long commitScnPosition, String rowId) {
        //return Collections.singletonMap(POSITION_FIELD, scnPosition);
        Map<String, String> offSet = new HashMap<String, String>();
        offSet.put(POSITION_FIELD, scnPosition.toString());
        offSet.put(COMMITSCN_POSITION_FIELD, commitScnPosition.toString());
        offSet.put(ROWID_POSITION_FIELD, rowId);
        return offSet;
    }


}