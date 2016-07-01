package com.manning.fia.c04;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Created by hari on 6/26/16.
 */


@SuppressWarnings("serial")
public class ApplyFunction implements WindowFunction<
        Tuple5<Long, String, String, String, String>,
        Tuple6<Long, Long, List<Long>, String, String, Long>,
        Tuple,
        TimeWindow> {
    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<Tuple5<Long, String, String, String, String>> inputs,
                      Collector<Tuple6<Long, Long, List<Long>, String, String, Long>> out) throws
            Exception {
        String section = ((Tuple2<String, String>) key).f0;
        String subSection = ((Tuple2<String, String>) key).f1;
        List<Long> eventIds = new ArrayList<Long>(0);
        long totalTimeSpent = 0;
        Iterator<Tuple5<Long, String, String, String, String>> iter = inputs.iterator();
        while(iter.hasNext()){
            Tuple5<Long, String, String, String, String> input =iter.next();
            eventIds.add(input.f0);
            long startTime = getTimeInMillis(input.f3);
            long endTime = getTimeInMillis(input.f4);
            totalTimeSpent += (endTime - startTime);
        }
        out.collect(new Tuple6<>(formatWindowTime(window.getStart()),formatWindowTime(window.getEnd()),
                   eventIds,
                   section,subSection,
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
