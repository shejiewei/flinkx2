package com.dtstack.flinkx.logminer.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OracleSourceConnector  {
  private static Logger log = LoggerFactory.getLogger(OracleSourceConnector.class);
  private OracleSourceConnectorConfig config;      

  public String version() {
    return VersionUtil.getVersion();
  }


  public void start(Map<String, String> map) {
    config = new OracleSourceConnectorConfig(map);    
    
    
    String dbName = config.getDbName();    
    if (dbName.equals("")){

    }
    String tableWhiteList = config.getTableWhiteList();
    if ((tableWhiteList == null)){

    }    
    //TODO: Add things you need to do to setup your connector.
  }



  public List<Map<String, String>> taskConfigs(int i) {
    //TODO: Define the individual task configurations that will be executed.
    ArrayList<Map<String,String>> configs = new ArrayList<>(1);
    //configs.add(config.originalsStrings());
    return configs;
    //throw new UnsupportedOperationException("This has not been implemented.");
  }

  public void stop() {
    //TODO: Do things that are necessary to stop your connector.    

  }


}