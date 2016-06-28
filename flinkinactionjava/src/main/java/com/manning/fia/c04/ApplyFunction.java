package com.manning.fia.c04;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class ApplyFunction implements WindowFunction<
        Tuple6<Long, String, String, Long, Long, Long>,
        Tuple6<String, String, Long, Long, Long, List<Long>>,
        Tuple,
        TimeWindow> {
    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<Tuple6<Long, String, String, Long, Long, Long>> inputs,
                      Collector<Tuple6<String, String, Long, Long, Long, List<Long>>> out) throws Exception {
        System.out.println("TIME WINDOW ==========>" + window);


        String section = ((Tuple2<String, String>) key).f0;
        String ss = ((Tuple2<String, String>) key).f1;

        long end = window.getEnd();

        List<Long> eventIds = new ArrayList<Long>(0);
        List<Tuple6<Long, String, String, Long, Long, Long>> list = IteratorUtils.toList(inputs.iterator());
        long firstEventStartTime = list.get(0).f3;
        long lastEventStartTime = list.get(list.size() - 1).f3;
        long totalTimeSpent = 0;
        for (Tuple6<Long, String, String, Long, Long, Long> input : list) {
            eventIds.add(input.f0);
            totalTimeSpent += input.f5;
        }
        out.collect(new Tuple6<>(section, ss, firstEventStartTime, lastEventStartTime, totalTimeSpent, eventIds));
    }
}
