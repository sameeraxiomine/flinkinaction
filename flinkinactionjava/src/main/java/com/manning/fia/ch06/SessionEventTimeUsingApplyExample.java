package com.manning.fia.ch06;

import java.util.List;

import com.manning.fia.c05.ApplyFunction;
import com.manning.fia.transformations.media.NewsFeedMapper3;
import com.manning.fia.utils.NewsFeedDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.joda.time.format.DateTimeFormat;

public class SessionEventTimeUsingApplyExample {

  private void executeJob(ParameterTool parameterTool) throws Exception{
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
        .getExecutionEnvironment();

    execEnv.setParallelism(parameterTool.getInt("parallelism", execEnv.getParallelism()));

    execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    final DataStream<String> dataStream;

    boolean isKafka = parameterTool.getBoolean("isKafka", false);

    if (isKafka) {
      dataStream = execEnv.addSource(NewsFeedDataSource.getKafkaDataSource(parameterTool));
    } else {
      dataStream = execEnv.addSource(NewsFeedDataSource.getCustomDataSource(parameterTool));
    }

    DataStream<Tuple5<Long, String, String, String, String>> selectDS = dataStream
        .map(new NewsFeedMapper3());

    KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS = selectDS
        .keyBy(1, 2);

    WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream = keyedDS
        .window(EventTimeSessionWindows.withGap(Time.seconds(10)));

    DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result = windowedStream
        .apply(new ApplyFunction());

    result.print();

    execEnv.execute("Event Time Session Window Apply");
  }

  private static class NewsFeedTimeStamp implements
      AssignerWithPeriodicWatermarks<Tuple5<Long, String, String, String, String>> {
    private static final long serialVersionUID = 1L;
    private long maxTimestamp = 0;
    private long priorTimestamp = 0;
    private long lastTimeOfWaterMarking = System.currentTimeMillis();

    @Override
    public Watermark getCurrentWatermark() {
      if (maxTimestamp == priorTimestamp) {
        long advance = (System.currentTimeMillis() - lastTimeOfWaterMarking);
        maxTimestamp += advance;// Start advancing
      }
      priorTimestamp = maxTimestamp;
      lastTimeOfWaterMarking = System.currentTimeMillis();
      return new Watermark(maxTimestamp);
    }

    @Override
    public long extractTimestamp(
        Tuple5<Long, String, String, String, String> element,
        long previousElementTimestamp) {
      long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
          .parseDateTime(element.f3).getMillis();
      maxTimestamp = Math.max(maxTimestamp, millis);
      return millis;
    }
  }

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    SessionEventTimeUsingApplyExample window = new SessionEventTimeUsingApplyExample();
    window.executeJob(parameterTool);
  }


}
