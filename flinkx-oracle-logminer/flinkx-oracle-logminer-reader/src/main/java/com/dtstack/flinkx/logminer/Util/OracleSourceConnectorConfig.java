package com.dtstack.flinkx.logminer.Util;

import org.apache.commons.collections.map.HashedMap;

import java.util.Map;

/**
 * Created by shejiewei on 2020/12/11.
 */
public class OracleSourceConnectorConfig {

  public static final String DB_NAME_ALIAS = "db.name.alias";
  public static final String TOPIC_CONFIG = "topic";
  public static final String DB_NAME_CONFIG = "db.name";
  public static final String DB_HOST_NAME_CONFIG = "db.hostname";
  public static final String DB_PORT_CONFIG = "db.port";
  public static final String DB_USER_CONFIG = "db.user";
  public static final String DB_USER_PASSWORD_CONFIG = "db.user.password";
  public static final String TABLE_WHITELIST = "table.whitelist";
  public static final String PARSE_DML_DATA = "parse.dml.data";
  public static final String DB_FETCH_SIZE = "db.fetch.size";
  public static final String RESET_OFFSET = "reset.offset";
  public static final String START_SCN = "start.scn";
  public static final String MULTITENANT = "multitenant";
  public static final String TABLE_BLACKLIST = "table.blacklist";
  public static final String DML_TYPES = "dml.types";

  private static Map<String, String> values=new HashedMap();

  public OracleSourceConnectorConfig(Map<String, String> parsedConfig) {
    values=parsedConfig;
  }


  public String getDbNameAlias(){ return values.get(DB_NAME_ALIAS);}
  public String getTopic(){ return values.get(TOPIC_CONFIG);}
  public String getDbName(){ return values.get(DB_NAME_CONFIG);}
  public String getDbHostName(){return values.get(DB_HOST_NAME_CONFIG);}
  public int getDbPort(){return Integer.parseInt(values.get(DB_PORT_CONFIG));}
  public String getDbUser(){return values.get(DB_USER_CONFIG);}
  public String getDbUserPassword(){return values.get(DB_USER_PASSWORD_CONFIG);}
  public String getTableWhiteList(){return values.get(TABLE_WHITELIST);}
  public Boolean getParseDmlData(){return Boolean.valueOf(values.get(PARSE_DML_DATA));}
  public int getDbFetchSize(){return Integer.parseInt(values.get(DB_FETCH_SIZE));}
  public Boolean getResetOffset(){return Boolean.valueOf(values.get(RESET_OFFSET));}
  public String getStartScn(){return values.get(START_SCN);}
  public Boolean getMultitenant() {return Boolean.valueOf(values.get(MULTITENANT));}
  public String getTableBlackList(){return values.get(TABLE_BLACKLIST);}
  public String getDMLTypes(){return values.get(DML_TYPES);}

  public static Map<String, String> getValues() {
    return values;
  }

  public static void setValues(Map<String, String> values) {
    OracleSourceConnectorConfig.values = values;
  }
}
