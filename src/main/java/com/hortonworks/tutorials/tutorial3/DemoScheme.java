package com.hortonworks.tutorials.tutorial3;

import java.util.ArrayList;
import java.util.Arrays;
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
  private boolean testMode = false;
  
  public static String[] orig_fields = {"_id", "user_id", "age", "status"};
  public static ArrayList<String> current_fields = new ArrayList<String>(Arrays.asList(orig_fields));
  
  public DemoScheme (String databaseName, String sourceMetasoureUrl) {
    this.databaseName = databaseName;
    this.sourceMetastoreUrl = sourceMetasoureUrl;
  }
  
  public DemoScheme (String databaseName, String sourceMetasoureUrl, boolean testMode) {
    this.databaseName = databaseName;
    this.sourceMetastoreUrl = sourceMetasoureUrl;
    this.testMode = testMode;
  }
  
  public void startSessionState(String metaStoreUrl) {
    if (! testMode) {
      HiveConf hcatConf = new HiveConf();
      hcatConf.setVar(HiveConf.ConfVars.METASTOREURIS, metaStoreUrl);
      hcatConf.set("hive.metastore.local", "false");
      hcatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
      hcatConf.set("hive.root.logger", "DEBUG,console");
      SessionState.start(hcatConf);
    }
  }

  public void execHiveDDL(String ddl) throws Exception {
    if (testMode) {
      System.err.println("Executing ddl = " + ddl);
    } else {
      LOG.info("Executing ddl = " + ddl);

      Driver hiveDriver = new Driver();
      CommandProcessorResponse response = hiveDriver.run(ddl);

      if (response.getResponseCode() > 0) {
        throw new Exception(response.getErrorMessage());
      }
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
        if (! current_fields.contains(attribute_name)) {
          new_attribute_names.add(attribute_name.toString());
        }
      }
      
      if (new_attribute_names.size() > 0) {
        for (String new_attribute_name: new_attribute_names) {

          String ddl = "alter table demo_messages_onefold add columns (" + new_attribute_name + " string)";
          startSessionState(sourceMetastoreUrl);
          
          try {
            execHiveDDL("use " + databaseName);
            execHiveDDL(ddl);
          } catch (Exception e) {
            String errorMessage = "Error exexcuting query[" + ddl.toString() + "]";
            LOG.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
          }
          
          current_fields.add(new_attribute_name);
          
        }
      }
      
      // turn message into "," delimited strings
      StringBuilder new_message_builder = new StringBuilder();
      for (String field: current_fields) {
        if (obj.has(field)) {
          String s = obj.get(field).toString();
          new_message_builder.append(s).append(",");
        } else {
          new_message_builder.append(",");
        }
      }
      
      return new Values(new_message_builder.toString());
      
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }

  public Fields getOutputFields() {
    return new Fields(FIELD_MESSAGE);
  }
  
  public static void test(DemoScheme ds, String s) {
    List<Object> a = ds.deserialize(s.getBytes());
    for (Object o: a) {
      System.err.println("values: " + o.toString());
    }
  }
  
  public static void main (String[] argv) {
    DemoScheme ds = new DemoScheme("", "", true);
    String s1 = "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\"}";
    String s2 = "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\",\"first_name\":\"first_name\"}";
    String s3 = "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\",\"last_name\":\"last_name\"}";
    
    test(ds, s1);
    test(ds, s2);
    test(ds, s3);
    
  }
}