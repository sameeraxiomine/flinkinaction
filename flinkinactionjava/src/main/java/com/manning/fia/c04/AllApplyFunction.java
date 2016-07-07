package com.manning.fia.c04;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by hari on 6/26/16.
 */


@SuppressWarnings("serial")
public class AllApplyFunction implements AllWindowFunction<
        String,
        String,        
        TimeWindow> {
    
    @Override
    public void apply(TimeWindow window,
            Iterable<String> inputs,
            Collector<String> out)
            throws Exception {        
        
        Iterator<String> iter = inputs.iterator();
        Map<String,Integer> countsByIPAddress = new HashMap<>();
        while(iter.hasNext()){
            String ipaddress =iter.next();
            if(!countsByIPAddress.containsKey(ipaddress)){
                countsByIPAddress.put(ipaddress, 0);   
            }
            countsByIPAddress.put(ipaddress, countsByIPAddress.get(ipaddress)+1);
        }
        for(Map.Entry<String, Integer>e:countsByIPAddress.entrySet()){
            if(e.getValue()>5){
                out.collect(e.getKey());    
            }            
        }
    }
}
