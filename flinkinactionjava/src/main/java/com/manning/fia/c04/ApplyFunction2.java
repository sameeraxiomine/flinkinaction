package com.manning.fia.c04;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

/**
 * Created by hari on 6/26/16.
 */


@SuppressWarnings("serial")
public class ApplyFunction2 implements WindowFunction<
        Tuple4<Long, String, String, String>,
        Tuple5<Long, Long, List<Long>, String, Long>,
        Tuple,
        TimeWindow> {
    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<Tuple4<Long, String, String, String>> inputs,
                      Collector<Tuple5<Long, Long, List<Long>, String, Long>> out) throws
            Exception {
        String topic = ((Tuple1<String>) key).f0;
        
        List<Long> eventIds = new ArrayList<Long>(0);
        long totalTimeSpent = 0;
        Iterator<Tuple4<Long, String, String, String>> iter = inputs.iterator();
        while(iter.hasNext()){
            Tuple4<Long, String, String, String> input =iter.next();
            eventIds.add(input.f0);
            long startTime = getTimeInMillis(input.f2);
            long endTime = getTimeInMillis(input.f3);
            totalTimeSpent += (endTime - startTime);
        }
        out.collect(new Tuple5<>(formatWindowTime(window.getStart()),formatWindowTime(window.getEnd()),
                   eventIds,
                   topic,
                    totalTimeSpent
                   ));
    }
    private long getTimeInMillis(String dtTime){
        return DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(dtTime).getMillis();
    }
    private long formatWindowTime(long millis){
        return Long.parseLong(DateTimeFormat.forPattern("yyyyMMddHHmmss").print(millis));
    }
}
