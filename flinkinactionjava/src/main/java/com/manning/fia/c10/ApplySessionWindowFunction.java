package com.manning.fia.c10;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

public class ApplySessionWindowFunction implements WindowFunction<
    Tuple6<String, Long, String, String, String, String>,
    Tuple5<Long, Long, String, List<Long>, Long>,
    Tuple,
    TimeWindow> {

  @Override
  public void apply(Tuple key,
                    TimeWindow timeWindow,
                    Iterable<Tuple6<String, Long, String, String, String, String>> inputs,
                    Collector<Tuple5<Long, Long, String, List<Long>, Long>> collector)
    throws Exception {

    List<Long> eventIds = new ArrayList<>();
    String subscriberId = ((Tuple1<String>) key).f0;
    long timespent = 0L;

    for (Tuple6<String, Long, String, String, String, String> input : inputs) {
      eventIds.add(input.f1);
      long startTime = getTimeInMillis(input.f4);
      long endTime = getTimeInMillis(input.f5);
      timespent += (endTime - startTime);
    }

    collector.collect(new Tuple5<>(
      formatWindowTime(timeWindow.getStart()),formatWindowTime(timeWindow.getEnd()),
      subscriberId,
      eventIds,
      timeWindow.getEnd() - timeWindow.getStart()
    ));

  }

  private long getTimeInMillis(String dtTime){
    return DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(dtTime).getMillis();
  }

  private long formatWindowTime(long millis) {
    return Long.parseLong(DateTimeFormat.forPattern("yyyyMMddHHmmss").print(millis));
  }
}
