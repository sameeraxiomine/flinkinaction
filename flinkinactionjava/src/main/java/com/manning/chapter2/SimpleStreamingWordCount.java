package com.manning.chapter2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Iterator;

public class SimpleStreamingWordCount {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    String[] lines = { "201603011201,#DCFlinkMeetup",
        "201603011202,#DcFlinkMeetup", "201603011203,#Flink",
        "201603011302,#Flink", "201603011302,#DCFlinkMeetup" };
    DataStream<String> source = execEnv.fromCollection(Arrays.asList(lines));

    DataStream<Tuple3<String, String, Integer>> counts = source.map(new Tokenizer())
        .keyBy(0, 1)
        .sum(2);
    Iterator<Tuple3<String,String,Integer>> iter = DataStreamUtils.collect(counts);
    for (Tuple3<String,String,Integer> line : Lists.newArrayList(iter)) {
      System.out.println(line.f0 +","+line.f1 + ","+line.f2);
    }
    counts.print();

        /*Write to a file*/
    //counts.writeAsCsv(filePath, "\n", ",");
    execEnv.execute();
  }

  @SuppressWarnings("serial")
  public static final class Tokenizer implements
      MapFunction<String, Tuple3<String, String, Integer>> {
    @Override
    public Tuple3<String, String, Integer> map(String value)
        throws Exception {
      String[] tokens = value.toLowerCase().split(",");
      String newDt = tokens[0].substring(0,10);
      String word = tokens[1].toLowerCase();
      return new Tuple3<>(newDt, word, 1);
    }
  }
}
