package com.hortonworks.tutorials.tutorial3;

import java.util.List;
import org.json.*;
import org.apache.log4j.Logger;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DemoScheme implements Scheme {

  public static final String FIELD_ID = "id";
  public static final String FIELD_MESSAGE = "message";
  private static final long serialVersionUID = -2990121166902741545L;
  private static final Logger LOG = Logger.getLogger(TruckScheme.class);

  public List<Object> deserialize(byte[] bytes) {
    try {
      String message = new String(bytes, "UTF-8");

      // parse id from object
      JSONObject obj = new JSONObject(message);
      String id = obj.getString("_id");

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