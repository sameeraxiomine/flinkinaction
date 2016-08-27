package com.manning.fia.c05;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class ApplyFunctionWithDomainObject implements WindowFunction<
        NewsFeed,
        Tuple6<Long, Long, List<Long>, String, String, Long>,
        Tuple,
        TimeWindow> {
    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<NewsFeed> inputs,
                      Collector<Tuple6<Long, Long, List<Long>, String, String, Long>> out) throws
            Exception {
        String section = ((Tuple2<String, String>) key).f0;
        String subSection = ((Tuple2<String, String>) key).f1;
        List<Long> eventIds = new ArrayList<Long>(0);
        long totalTimeSpent = 0;
        Iterator<NewsFeed> iter = inputs.iterator();
        while (iter.hasNext()) {
            NewsFeed input = iter.next();
            eventIds.add(input.getEventId());
            long startTime = getTimeInMillis(input.getStartTimeStamp());
            long endTime = getTimeInMillis(((NewsFeed) input).getEndTimeStamp());
            totalTimeSpent += (endTime - startTime);
        }
        if (!section.isEmpty()) {
            out.collect(new Tuple6<>(formatWindowTime(window.getStart()), formatWindowTime(window.getEnd()),
                    eventIds,
                    section, subSection,
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
