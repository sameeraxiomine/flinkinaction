package com.manning.fia.c05;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.java.tuple.*;
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
public class SectionAggregator implements WindowFunction<
        Tuple3<List<Long>, String, Long>,
        Tuple5<Long, Long, List<Long>,  String, Long>,
        Tuple,
        TimeWindow> {
    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<Tuple3<List<Long>, String, Long>> inputs,
                      Collector<Tuple5<Long, Long, List<Long>, String, Long>> out) throws
            Exception {
        String section = ((Tuple1<String>) key).f0;

        List<Long> eventIds = new ArrayList<Long>(0);

        long totalTimeSpent = 0;
        Iterator<Tuple3<List<Long>, String, Long>> iter = inputs.iterator();
        while (iter.hasNext()) {
            Tuple3<List<Long>, String, Long> input = iter.next();

            eventIds.addAll(input.f0);
            totalTimeSpent += input.f2;


        }
        if (!section.isEmpty()) {
            out.collect(new Tuple5<>(formatWindowTime(window.getStart()), formatWindowTime(window.getEnd()),
                    eventIds,
                    section,
                    totalTimeSpent

            ));
        }

    }

    private long getTimeInMillis(String dtTime) {
        return DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(dtTime).getMillis();
    }

    private long formatWindowTime(long millis) {
        return Long.parseLong(DateTimeFormat.forPattern("yyyyMMddHHmmss").print(millis));
    }
}
