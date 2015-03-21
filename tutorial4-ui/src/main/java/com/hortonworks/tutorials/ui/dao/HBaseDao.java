package com.hortonworks.tutorials.ui.dao;

import com.hortonworks.tutorials.ui.vo.Coordinate;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

//@todo change to Spring Component (spring-hadoop)
//@Component("hbasedao")
public class HBaseDao
{
     //TABLES
    private static final String EVENTS_TABLE_NAME =  "truck_events";
    private static final String EVENTS_COUNT_TABLE_NAME = "driver_dangerous_events";

    //CF
    private static final byte[] CF_EVENTS_TABLE = Bytes.toBytes("events");
    private static final byte[] CF_EVENTS_COUNT_TABLE = Bytes.toBytes("count");

    //COL
    private static final byte[] COL_COUNT_VALUE = Bytes.toBytes("value");

    private static final byte[] COL_DRIVER_ID = Bytes.toBytes("d");
    private static final byte[] COL_TRUCK_ID = Bytes.toBytes("t");
    private static final byte[] COL_EVENT_TIME = Bytes.toBytes("tim");
    private static final byte[] COL_EVENT_TYPE = Bytes.toBytes("e");
    private static final byte[] COL_LATITUDE = Bytes.toBytes("la");
    private static final byte[] COL_LONGITUDE = Bytes.toBytes("lo");
    
    //
    private static final Map<String, String> driverInfo = new HashMap<String, String>();
    private static final Map<String, String> currDriverRouteId = new HashMap<String, String>();
    private static final Map<String, String> routeIdInfo = new HashMap<String, String>();
    
    
    
    static 
    {
        driverInfo.put ("11", "Jim C");
        driverInfo.put ("12", "Jackie K");
        driverInfo.put ("13",  "Kevin B");
        driverInfo.put ("14",  "Jhon T");
        
        currDriverRouteId.put("11", "001");
        currDriverRouteId.put("12", "002");
        currDriverRouteId.put("13", "003");
        currDriverRouteId.put("14", "004");
        
        routeIdInfo.put("001", "route 17");
        routeIdInfo.put("002", "route 17k"); 
        routeIdInfo.put("003", "route 208");
        routeIdInfo.put("004", "route 27");
    }
    
    //returns the last n coordinates from HBase table.
    public List<Coordinate> getCoordinates(int batchSize) throws IOException 
    {
	    HTable eventsTbl = null;
        HTable eventsCntTbl = null;
        ResultScanner scanner = null;
        ArrayList<String> driverIDList = new ArrayList<String>();
        
        List<Coordinate> result = new ArrayList<Coordinate>(batchSize);
       // SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try
        {
            Configuration conf =  HBaseConfiguration.create();
            conf.addResource(new URL("http://localhost:60010/conf"));
        
            eventsTbl =  new HTable(conf, EVENTS_TABLE_NAME);
            eventsCntTbl = new HTable(conf, EVENTS_COUNT_TABLE_NAME);
            
            Scan scan = new Scan();
            
            scanner = eventsTbl.getScanner(scan);
            
            int cnt = 0;
            for (Result r = scanner.next(); r != null && cnt < batchSize; ++cnt, r=scanner.next())
            {
                String driverId = Bytes.toString(r.getValue(CF_EVENTS_TABLE, COL_DRIVER_ID)).trim();
                
				if(driverIDList.contains(driverId))
                	continue;
                
                driverIDList.add(driverId);
                
                String truckId = Bytes.toString(r.getValue(CF_EVENTS_TABLE, COL_TRUCK_ID)).trim();
                String eventType = Bytes.toString(r.getValue(CF_EVENTS_TABLE, COL_EVENT_TYPE)).trim();
                long eventTime = Bytes.toLong(r.getValue(CF_EVENTS_TABLE, COL_EVENT_TIME));
                String latitude = Bytes.toString(r.getValue(CF_EVENTS_TABLE, COL_LATITUDE)).trim();
                String longitude = Bytes.toString(r.getValue(CF_EVENTS_TABLE,COL_LONGITUDE)).trim();
                
                long count = 0l;
                
                Result cntRes = eventsCntTbl.get(new Get(Bytes.toBytes(driverId)));
                if (!cntRes.isEmpty()) 
                {
                    count = Bytes.toLong(cntRes.getValue(CF_EVENTS_COUNT_TABLE, COL_COUNT_VALUE));
                }
                
                Coordinate c = new Coordinate();
                c.setDriver_id(driverId);
                c.setDriver_name(driverInfo.get(driverId));
                c.setLatitude(latitude);
                c.setLongitude(longitude);
                c.setRoute_id(currDriverRouteId.get(driverId));
                c.setRoute_name(routeIdInfo.get(c.getRoute_id()));
                c.setTimestamp((new java.util.Date(eventTime)).toString());
                c.setTotal_violations(String.valueOf(count));
                c.setTruck_id(truckId);
                c.setViolation(eventType);

                result.add(c);
                               
            }
            
            return result;
        }
        finally
        {
            if (scanner != null)
            {
                scanner.close();
            }
            
            if (eventsCntTbl != null)
            {
                eventsCntTbl.close();
            }
            
            if (eventsTbl != null)
            {
                eventsTbl.close(); 
            }
        }
        
    }
    
    public static void main (String str[]) throws IOException
    {
        (new HBaseDao()).getCoordinates(1000);
    }
    
}