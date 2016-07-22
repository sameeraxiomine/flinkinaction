package com.manning.fia.c05;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by hari on 6/26/16.
 */


@SuppressWarnings("serial")
public class MyApplyFunction implements WindowFunction<
        Tuple3<String, String,Long>,
        Tuple3<Long, Long, List<String>>,
        Tuple,
        TimeWindow> {
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<Tuple3<String, String,Long>> inputs,
                      Collector<Tuple3<Long,Long, List<String>>> out) throws
            Exception {
        List<String> eventIds = new ArrayList<String>(0);
        Iterator<Tuple3<String, String,Long>> iter = inputs.iterator();
        while(iter.hasNext()){
        	Tuple3<String, String,Long> input =iter.next();
            eventIds.add(input.f1);
            long time = input.f2;
        }
        out.collect(new Tuple3<>(formatWindowTime(window.getStart()),formatWindowTime(window.getEnd()),
                   eventIds
                   ));
    }
    private long formatWindowTime(long millis){
        return Long.parseLong(DateTimeFormat.forPattern("yyyyMMddHHmmss").print(millis));
    }
}
