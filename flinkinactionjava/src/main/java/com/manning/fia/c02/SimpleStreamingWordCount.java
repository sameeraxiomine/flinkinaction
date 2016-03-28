package com.manning.fia.c02;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SimpleStreamingWordCount {
  public static int PORT_NO=9500;
  public static StreamServer getStreamServer(List<String> hashTagsList){
    String[] hashtags = new String[hashTagsList.size()];
    hashtags = hashTagsList.toArray(hashtags);
    return new StreamServer(PORT_NO,hashtags,2,1000);
  }

public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment streamingExecEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
    streamingExecEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);    
    List<String> hashTagsList = FileUtils.readLines(new File("src/main/resources/sample/streaminghashtags.txt"));

    StreamServer server = getStreamServer(hashTagsList);
    
    server.startServer();
    Thread.sleep(1000);//Allow server to start
    DataStream<String> source1 = streamingExecEnv.addSource(new SocketTextStreamFunction("127.0.0.1",PORT_NO, '\n', 1));
    DataStream<Tuple2<String, Integer>> counts1 = source1.map(new Tokenizer())
            .keyBy(0)
            .sum(1);    
     counts1.printToErr();
     streamingExecEnv.execute("Default is rolling reduce. Sum keep getting added per key.Batch as a special case of streaming");
    
     server.startServer();
     DataStream<String> source2 = streamingExecEnv.addSource(new SocketTextStreamFunction("127.0.0.1",PORT_NO, '\n', 1));
     DataStream<Tuple2<String, Integer>> counts2 = source2.map(new Tokenizer())
             .keyBy(0)   
             .timeWindow(Time.seconds(5))
             .sum(1);    
         counts2.printToErr();
     streamingExecEnv.execute("Tumbling window of 5 seconds");

     server.startServer();
     DataStream<String> source3 = streamingExecEnv.addSource(new SocketTextStreamFunction("127.0.0.1",PORT_NO, '\n', 1));
     DataStream<Tuple2<String, Integer>> counts3 = source3.map(new Tokenizer())
             .keyBy(0)   
             .timeWindow(Time.seconds(15),Time.seconds(5))
             .sum(1);    
         counts3.printToErr();
     streamingExecEnv.execute("Sliding window of word count in last 15 seconds, in each 5 second interval");
     
     server.startServer();
     DataStream<String> source4 = streamingExecEnv.addSource(new SocketTextStreamFunction("127.0.0.1",PORT_NO, '\n', 1));
     DataStream<Tuple2<String, Integer>> counts4 = source4.map(new Tokenizer())
             .keyBy(0)   
             .countWindow(5)
             .sum(1);    
     counts4.printToErr();
     streamingExecEnv.execute("Count windows triggered when count reaches 5 for any key.Does not fire again for that key");

     server.startServer();
     DataStream<String> source5 = streamingExecEnv.addSource(new SocketTextStreamFunction("127.0.0.1",PORT_NO, '\n', 1));     
     DataStream<Tuple2<String, Integer>> counts5 = source5.map(new Tokenizer())
             .keyBy(0)   
             .countWindow(15)
             .sum(1);    
     counts5.printToErr();
     streamingExecEnv.execute("Count windows triggered when count reaches 15 for any key");
     

    }

  @SuppressWarnings("serial")
  public static final class Tokenizer implements
      MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String value)
        throws Exception {
      String[] tokens = value.toLowerCase().split(",");
      String word = tokens[1].toLowerCase();
      return new Tuple2<>(word, 1);
    }
  }
}
