package com.dtstack.flinkx.logminer.Util;

/**
 * Created by shejiewei on 2020/12/11.
 */
public class VersionUtil {
  public static String getVersion() {
    try {
      return VersionUtil.class.getPackage().getImplementationVersion();
    } catch(Exception ex){
      return "0.0.0.0";
    }
  }
}
