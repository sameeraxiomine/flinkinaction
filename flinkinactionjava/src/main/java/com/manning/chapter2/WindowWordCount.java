package com.manning.chapter2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Streaming Window Word Count Example
 */
public class WindowWordCount {

  public static void main(String[] args) throws Exception {
    // Get an instance of the Streaming Execution Environment
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    // Create a DataStream from the text input
    DataStream<String> lines =
        env.readTextFile("src/main/resources/wordcount/input.txt");

    int windowSize = 250;
    int slideSize = 10;

    DataStream<Tuple2<String, Integer>> counts =
        // Splits the words up into Tuple2<Word,1>
        lines.flatMap(new LineSplitter())
            //group by the tuple field "0"
            .keyBy(0)
            // create a Window of 'windowSize' records and slide window by 'slideSize' records
            // KeyStream -> WindowStream
            .countWindow(windowSize, slideSize)
            // and sum up tuple field "1"
            .sum(1)
            // consider only word counts > 1
            .filter(new WordCountFilter());

    counts.writeAsText("/tmp/windowFilterCount", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

    // Process the DataStream
    env.execute("Streaming Word Count");
  }

  public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
      for (String word : s.split(" ")) {
        collector.collect(new Tuple2<>(word, 1));
      }
    }
  }

  public static class WordCountFilter implements FilterFunction<Tuple2<String, Integer>> {
    @Override
    public boolean filter(Tuple2<String, Integer> tuple2) throws Exception {
      return tuple2.f1 > 1;
    }
  }
}
