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
import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class ApplyFunction implements WindowFunction<
        Tuple5<Long, String, String, String, String>,
        Tuple8<String, String, String, String, Long, Long, Long, List<Long>>,
        Tuple,
        TimeWindow> {
    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<Tuple5<Long, String, String, String, String>> inputs,
                      Collector<Tuple8<String, String, String, String, Long, Long, Long, List<Long>>> out) throws
            Exception {

        String section = ((Tuple2<String, String>) key).f0;
        String subSection = ((Tuple2<String, String>) key).f1;
        List<Long> eventIds = new ArrayList<Long>(0);
        List<Tuple5<Long, String, String, String, String>> list = IteratorUtils.toList(inputs.iterator());
        String firstEventStartTime = list.get(0).f3;
        String lastEventStartTime = list.get(list.size() - 1).f3;
        long totalTimeSpent = 0;
        for (Tuple5<Long, String, String, String, String> input : list) {
            eventIds.add(input.f0);
            long startTime = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(input.f3).getMillis();
            long endTime = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(input.f4).getMillis();
            totalTimeSpent += endTime - startTime;
        }
        long windowStart = window.getStart();
        long windowEnd = window.getEnd();


        out.collect(new Tuple8<>(
                    section,
                    subSection,
                    firstEventStartTime,
                    lastEventStartTime,
                    windowStart,
                    windowEnd,
                    totalTimeSpent,
                    eventIds));
    }
}
