package com.hortonworks.tutorials.tutorial3;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.json.*;

public class DemoJson {

  public static void main(String[] argv) {
    try {

      JSONObject obj = new JSONObject(
          "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\"}");

      JSONArray attribute_names = obj.names();
      for (int i = 0; i < attribute_names.length(); i++) {
        Object attribute_name = (String) attribute_names.get(i);
        System.err.println(attribute_name);
      }

      String configFileLocation = "demo_topology.properties";
      Properties topologyConfig = new Properties();
      try {
        topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
      } catch (FileNotFoundException e) {
        throw e;
      } catch (IOException e) {
        throw e;
      }     
      
      String ddl = "alter table demo_messages_onefold add columns (first_name string)";
      String sourceMetastoreUrl = topologyConfig.getProperty("hive.metastore.url");
      String databaseName = topologyConfig.getProperty("hive.database.name");

      startSessionState(sourceMetastoreUrl);

      try {
        execHiveDDL("use " + databaseName);
        execHiveDDL("add jar /home/jkpchang/hive-serdes-1.0-SNAPSHOT.jar");
        execHiveDDL(ddl);
      } catch (Exception e) {
        String errorMessage = "Error exexcuting query[" + ddl.toString() + "]";
        throw new RuntimeException(errorMessage, e);
      }

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static void startSessionState(String metaStoreUrl) {
    HiveConf hcatConf = new HiveConf();
    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUrl);
    hcatConf.set("hive.metastore.local", "false");
    hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hcatConf.set("hive.root.logger", "DEBUG,console");
    SessionState.start(hcatConf);
  }

  public static void execHiveDDL(String ddl) throws Exception {

    Driver hiveDriver = new Driver();
    CommandProcessorResponse response = hiveDriver.run(ddl);

    if (response.getResponseCode() > 0) {
      throw new Exception(response.getErrorMessage());
    }
  }

}