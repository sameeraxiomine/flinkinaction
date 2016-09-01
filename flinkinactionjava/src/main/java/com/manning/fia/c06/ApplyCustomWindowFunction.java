package com.manning.fia.c06;

import com.manning.fia.model.media.NewsFeed;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

public class ApplyCustomWindowFunction implements WindowFunction<NewsFeed,
  Tuple6<Long, Long, List<Long>, String, String, Long>,
  Tuple2<String, String>, TimeWindow> {

  @Override
  public void apply(Tuple2<String, String> key,
                    TimeWindow window,
                    Iterable<NewsFeed> inputs,
                    Collector<Tuple6<Long, Long, List<Long>, String, String, Long>> out) throws Exception {
    String section = key.f0;
    String subSection = key.f1;
    List<Long> eventIds = new ArrayList<>(0);

    long totalTimeSpent = 0;
    for (NewsFeed input : inputs) {
      if (input != null) {
        eventIds.add(input.getEventId());
        long startTime = getTimeInMillis(input.getStartTimeStamp());
        long endTime = getTimeInMillis(input.getEndTimeStamp());
        totalTimeSpent += (endTime - startTime);
      }
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
