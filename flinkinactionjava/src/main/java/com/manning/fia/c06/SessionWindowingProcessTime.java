package com.manning.fia.c06;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by smarthi on 8/29/16.
 */
public class SessionWindowingProcessTime {

  @SuppressWarnings("serial")
  public static void main(String[] args) throws Exception {

//    final ParameterTool params = ParameterTool.fromArgs(args);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//    env.getConfig().setGlobalJobParameters(params);
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    env.setParallelism(1);

    final boolean fileOutput = false;

    final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

    input.add(new Tuple3<>("a", 1L, 1));
    input.add(new Tuple3<>("b", 1L, 1));
    input.add(new Tuple3<>("b", 3L, 1));
    input.add(new Tuple3<>("b", 5L, 1));
    input.add(new Tuple3<>("c", 6L, 1));
    // We expect to detect the session "a" earlier than this point (the old
    // functionality can only detect here when the next starts)
    input.add(new Tuple3<>("a", 10L, 1));
    // We expect to detect session "b" and "c" at this point as well
    input.add(new Tuple3<>("c", 11L, 1));

    DataStream<Tuple3<String, Long, Integer>> source = env.fromCollection(input);

    // We create sessions for each id with max timeout of 3 time units
    DataStream<Tuple3<String, Long, Integer>> aggregated = source
      .keyBy(0)
      .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(3L)))
      .sum(2);

    if (fileOutput) {
      aggregated.writeAsText("/temp/samplesession.txt");
    } else {
      System.out.println("Printing result to stdout. Use --output to specify output path.");
      aggregated.print();
    }

    env.execute();
  }
}
