package com.manning.fia.ch06;

import com.manning.fia.utils.NewsFeedDataSource;
import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class SessionProcessingTimeUsingApplyExample {

  private void executeJob(ParameterTool parameterTool) throws Exception{
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
        .getExecutionEnvironment();

    execEnv.setParallelism(parameterTool.getInt("parallelism", 1));

    execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    final DataStream<String> dataStream;

    boolean isKafka = parameterTool.getBoolean("isKafka", false);

    if (isKafka) {
      dataStream = execEnv.addSource(NewsFeedDataSource.getKafkaDataSource(parameterTool));
    } else {
      dataStream = execEnv.addSource(NewsFeedDataSource.getCustomDataSource(parameterTool));
    }

    DataStream<Tuple6<String, Long, String, String, String, String>> selectDS = dataStream
        .map(new NewsFeedSubscriberMapper());

    // Key by the SubscriberId
    KeyedStream<Tuple6<String, Long, String, String, String, String>, String> keyedDS =
      selectDS.keyBy(
        new KeySelector<Tuple6<String, Long, String, String, String, String>, String>() {
          @Override
          public String getKey(Tuple6<String, Long, String, String, String, String> tuple6) throws Exception {
            return tuple6.f0;
          }
        });

    // Create a Session Window for each subscriberId
    WindowedStream<Tuple6 < String, Long, String, String, String, String >, String, TimeWindow > windowedStream =
      keyedDS.window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(3)));

    // Aggregate the data
    DataStream<Tuple5<String, List<Long>, Long, Long, Long>> result = windowedStream
        .apply(new ApplySessionWindowFunction());

    result.print();

    execEnv.execute("Processing Time Session Window Apply");
  }

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    SessionProcessingTimeUsingApplyExample window = new SessionProcessingTimeUsingApplyExample();
    window.executeJob(parameterTool);
  }
}
