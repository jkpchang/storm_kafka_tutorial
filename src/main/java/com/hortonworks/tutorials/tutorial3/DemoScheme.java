package com.hortonworks.tutorials.tutorial3;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DemoScheme implements Scheme {

  public static final String FIELD_ID = "id";
  public static final String FIELD_MESSAGE = "message";
  private static final long serialVersionUID = -2990121166902741545L;
  private static final Logger LOG = Logger.getLogger(TruckScheme.class);
  
  private String databaseName;
  private String sourceMetastoreUrl;
//  public static HashMap current_fields = new HashMap();
  
  public DemoScheme (String databaseName, String sourceMetasoureUrl) {
    this.databaseName = databaseName;
    this.sourceMetastoreUrl = sourceMetasoureUrl;
  }
  
  public void startSessionState(String metaStoreUrl) {
    HiveConf hcatConf = new HiveConf();
    hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUrl);
    hcatConf.set("hive.metastore.local", "false");
    hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hcatConf.set("hive.root.logger", "DEBUG,console");
    SessionState.start(hcatConf);
  }

  public void execHiveDDL(String ddl) throws Exception {
    LOG.info("Executing ddl = " + ddl);

    Driver hiveDriver = new Driver();
    CommandProcessorResponse response = hiveDriver.run(ddl);

    if (response.getResponseCode() > 0) {
      throw new Exception(response.getErrorMessage());
    }
  }

  public List<Object> deserialize(byte[] bytes) {
    try {
      String message = new String(bytes, "UTF-8");

      // parse id from object
      JSONObject obj = new JSONObject(message);
      
      // get all top level attribute names from object
      JSONArray attribute_names = obj.names();
      List<String> new_attribute_names = new ArrayList<String>();
      for (int i = 0; i < attribute_names.length(); i++) {
        Object attribute_name = (String) attribute_names.get(i);
        if (attribute_name != "_id" && attribute_name != "user_id" && attribute_name != "age" && attribute_name != "status") {
          new_attribute_names.add(attribute_name.toString());
        }
      }
      
      if (new_attribute_names.size() > 0) {
        for (String new_attribute_name: new_attribute_names) {

          String ddl = "alter table demo_messages_onefold add columns (" + new_attribute_name + " string)";
          startSessionState(sourceMetastoreUrl);
          
          try {
            execHiveDDL("use " + databaseName);
            execHiveDDL("add jar /home/jkpchang/hive-serdes-1.0-SNAPSHOT.jar");
            execHiveDDL(ddl);
          } catch (Exception e) {
            String errorMessage = "Error exexcuting query[" + ddl.toString() + "]";
            LOG.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
          }
          
        }
      }

      return new Values(message);
      
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }

  public Fields getOutputFields() {
    return new Fields(FIELD_MESSAGE);
  }
}