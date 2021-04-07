package com.dtstack.flinkx.logminer.reader;


import java.io.Serializable;


/**
 * Created by shejiewei on 2020/12/11.
 */
public class LogminerConfig implements Serializable {

    public  String DB_NAME_ALIAS = "db.name.alias";
    public  String TOPIC_CONFIG = "topic";
    public  String DB_NAME_CONFIG = "db.name";
    public  String DB_HOST_NAME_CONFIG = "db.hostname";
    public  String DB_PORT_CONFIG = "db.port";
    public  String DB_USER_CONFIG = "db.user";
    public  String DB_USER_PASSWORD_CONFIG = "db.user.password";
    public  String TABLE_WHITELIST = "table.whitelist";
    public  String PARSE_DML_DATA = "parse.dml.data";
    public  String DB_FETCH_SIZE = "db.fetch.size";
    public  String RESET_OFFSET = "reset.offset";
    public  String START_SCN = "start.scn";
    public  String MULTITENANT = "multitenant";
    public  String TABLE_BLACKLIST = "table.blacklist";
    public  String DML_TYPES = "dml.types";

    public String getDB_NAME_ALIAS() {
        return DB_NAME_ALIAS;
    }

    public void setDB_NAME_ALIAS(String DB_NAME_ALIAS) {
        this.DB_NAME_ALIAS = DB_NAME_ALIAS;
    }

    public String getTOPIC_CONFIG() {
        return TOPIC_CONFIG;
    }

    public void setTOPIC_CONFIG(String TOPIC_CONFIG) {
        this.TOPIC_CONFIG = TOPIC_CONFIG;
    }

    public String getDB_NAME_CONFIG() {
        return DB_NAME_CONFIG;
    }

    public void setDB_NAME_CONFIG(String DB_NAME_CONFIG) {
        this.DB_NAME_CONFIG = DB_NAME_CONFIG;
    }

    public String getDB_HOST_NAME_CONFIG() {
        return DB_HOST_NAME_CONFIG;
    }

    public void setDB_HOST_NAME_CONFIG(String DB_HOST_NAME_CONFIG) {
        this.DB_HOST_NAME_CONFIG = DB_HOST_NAME_CONFIG;
    }

    public String getDB_PORT_CONFIG() {
        return DB_PORT_CONFIG;
    }

    public void setDB_PORT_CONFIG(String DB_PORT_CONFIG) {
        this.DB_PORT_CONFIG = DB_PORT_CONFIG;
    }

    public String getDB_USER_CONFIG() {
        return DB_USER_CONFIG;
    }

    public void setDB_USER_CONFIG(String DB_USER_CONFIG) {
        this.DB_USER_CONFIG = DB_USER_CONFIG;
    }

    public String getDB_USER_PASSWORD_CONFIG() {
        return DB_USER_PASSWORD_CONFIG;
    }

    public void setDB_USER_PASSWORD_CONFIG(String DB_USER_PASSWORD_CONFIG) {
        this.DB_USER_PASSWORD_CONFIG = DB_USER_PASSWORD_CONFIG;
    }

    public String getTABLE_WHITELIST() {
        return TABLE_WHITELIST;
    }

    public void setTABLE_WHITELIST(String TABLE_WHITELIST) {
        this.TABLE_WHITELIST = TABLE_WHITELIST;
    }

    public String getPARSE_DML_DATA() {
        return PARSE_DML_DATA;
    }

    public void setPARSE_DML_DATA(String PARSE_DML_DATA) {
        this.PARSE_DML_DATA = PARSE_DML_DATA;
    }

    public String getDB_FETCH_SIZE() {
        return DB_FETCH_SIZE;
    }

    public void setDB_FETCH_SIZE(String DB_FETCH_SIZE) {
        this.DB_FETCH_SIZE = DB_FETCH_SIZE;
    }

    public String getRESET_OFFSET() {
        return RESET_OFFSET;
    }

    public void setRESET_OFFSET(String RESET_OFFSET) {
        this.RESET_OFFSET = RESET_OFFSET;
    }

    public String getSTART_SCN() {
        return START_SCN;
    }

    public void setSTART_SCN(String START_SCN) {
        this.START_SCN = START_SCN;
    }

    public String getMULTITENANT() {
        return MULTITENANT;
    }

    public void setMULTITENANT(String MULTITENANT) {
        this.MULTITENANT = MULTITENANT;
    }

    public String getTABLE_BLACKLIST() {
        return TABLE_BLACKLIST;
    }

    public void setTABLE_BLACKLIST(String TABLE_BLACKLIST) {
        this.TABLE_BLACKLIST = TABLE_BLACKLIST;
    }

    public String getDML_TYPES() {
        return DML_TYPES;
    }

    public void setDML_TYPES(String DML_TYPES) {
        this.DML_TYPES = DML_TYPES;
    }
}
