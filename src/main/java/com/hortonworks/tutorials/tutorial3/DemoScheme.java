package com.hortonworks.tutorials.tutorial3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

  public static String[] orig_fields = { "_id", "user_id", "age", "status" };
  public static ArrayList<String> current_fields = new ArrayList<String>(Arrays.asList(orig_fields));
  public static HashMap<String, String> data_type_map = new HashMap<String, String>();

  public DemoScheme(String databaseName, String sourceMetasoureUrl) {
    this.databaseName = databaseName;
    this.sourceMetastoreUrl = sourceMetasoureUrl;
    initialize();
  }

  public DemoScheme(String databaseName, String sourceMetasoureUrl, boolean testMode) {
    this.databaseName = databaseName;
    this.sourceMetastoreUrl = sourceMetasoureUrl;
    this.testMode = testMode;
    initialize();
  }

  public void initialize() {
    for (String field : current_fields) {
      if (field.equals("age")) {
        data_type_map.put(field, "bigint");
      } else {
        data_type_map.put(field, "string");
      }
    }
  }

  public void startSessionState(String metaStoreUrl) {
    if (!testMode) {
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

  public boolean isDecimal(String str) {
    try {
      double d = Double.parseDouble(str);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  public boolean isInteger(String s) {
    return isInteger(s, 10);
  }

  public boolean isInteger(String s, int radix) {
    if (s.isEmpty())
      return false;
    for (int i = 0; i < s.length(); i++) {
      if (i == 0 && s.charAt(i) == '-') {
        if (s.length() == 1)
          return false;
        else
          continue;
      }
      if (Character.digit(s.charAt(i), radix) < 0)
        return false;
    }
    return true;
  }

  public String getDataType(String value) {
    if (value == null) {
      return "string";
    } else if (isInteger(value)) {
      return "bigint";
    } else if (isDecimal(value)) {
      return "decimal";
    } else {
      return "string";
    }
  }

  public String compareDataType(String data_type_1, String data_type_2) {
    if (data_type_1 == "string" || data_type_2 == "string") {
      return "string";
    } else if (data_type_1 == "decimal" || data_type_2 == "decimal") {
      return "decimal";
    } else {
      return "bigint";
    }
  }

  public void addColumn(String column_name, String data_type) {
    String ddl = "alter table demo_messages_onefold add columns (" + column_name + " " + data_type + ")";
    startSessionState(sourceMetastoreUrl);

    try {
      execHiveDDL("use " + databaseName);
      execHiveDDL(ddl);
    } catch (Exception e) {
      String errorMessage = "Error exexcuting query[" + ddl.toString() + "]";
      LOG.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }

    current_fields.add(column_name);
    data_type_map.put(column_name, data_type);
  }
  
  public void updateColumn(String column_name, String data_type) {
    String ddl = "alter table demo_messages_onefold change " + column_name + " " + column_name + " " +  data_type;
    startSessionState(sourceMetastoreUrl);

    try {
      execHiveDDL("use " + databaseName);
      execHiveDDL(ddl);
    } catch (Exception e) {
      String errorMessage = "Error exexcuting query[" + ddl.toString() + "]";
      LOG.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }

    data_type_map.put(column_name, data_type);
  }

  public List<Object> deserialize(byte[] bytes) {
    try {
      String message = new String(bytes, "UTF-8");

      // parse id from object
      JSONObject jsonObj = new JSONObject(message);

      // get all top level attribute names from object
      JSONArray attribute_names = jsonObj.names();

      for (int i = 0; i < attribute_names.length(); i++) {
        
        String attribute_name = (String) attribute_names.get(i);
        String attribute_value = jsonObj.getString(attribute_name);
        String attribute_data_type = getDataType(attribute_value);

        if (!current_fields.contains(attribute_name)) {
          addColumn(attribute_name, attribute_data_type);
        } else {
          String old_data_type = data_type_map.get(attribute_name);
          if (! old_data_type.equals(attribute_data_type)) {
            updateColumn(attribute_name, compareDataType(attribute_data_type, old_data_type));
          }
        }
      }

      // turn message into "," delimited strings
      StringBuilder new_message_builder = new StringBuilder();
      for (String field : current_fields) {
        if (jsonObj.has(field)) {
          String s = jsonObj.get(field).toString();
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
    for (Object o : a) {
      System.err.println("values: " + o.toString());
    }
  }

  public static void main(String[] argv) {
    DemoScheme ds = new DemoScheme("", "", true);
    String s1 = "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\"}";
    String s2 = "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\",\"first_name\":\"first_name\"}";
    String s3 = "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\",\"last_name\":\"last_name\"}";
    String s4 = "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\",\"zip_code\":1234}";
    String s5 = "{\"_id\":\"551f0ba0ca83908566b20498\",\"user_id\":\"bcd001\",\"age\":\"45\",\"status\":\"A\",\"zip_code\":\"M1K5A5\"}";

    test(ds, s1);
    test(ds, s2);
    test(ds, s3);
    test(ds, s4);
    test(ds, s5);

  }
}