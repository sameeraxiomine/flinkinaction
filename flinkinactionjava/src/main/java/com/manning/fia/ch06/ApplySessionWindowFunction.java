package com.manning.fia.ch06;

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
    Tuple5<String, List<Long>, Long, Long, Long>, Tuple, TimeWindow> {

  @Override
  public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple6<String, Long, String, String, String, String>> inputs,
                    Collector<Tuple5<String, List<Long>, Long, Long, Long>> collector) throws Exception {

    String subscriberId = ((Tuple1<String>) key).f0;
    long sessionTime;
    List<Long> eventIds = new ArrayList<>();

    for (Tuple6<String, Long, String, String, String, String> input : inputs) {
      eventIds.add(input.f1);
    }

    long sessionStartTime = timeWindow.getStart();
    long sessionEndTime = timeWindow.getEnd();
    sessionTime = sessionEndTime - sessionStartTime;

    collector.collect(new Tuple5<>(
      subscriberId,
      eventIds,
      formatWindowTime(sessionStartTime),
      formatWindowTime(sessionEndTime),
      sessionTime
    ));

  }

  private long formatWindowTime(long millis) {
    return Long.parseLong(DateTimeFormat.forPattern("yyyyMMddHHmmss").print(millis));
  }
}
