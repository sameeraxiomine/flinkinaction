package com.manning.fia.c06;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.utils.NewsFeedDataSource;
import java.util.List;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.joda.time.format.DateTimeFormat;

public class CustomWindowAssignerExample {

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

    DataStream<NewsFeed> selectDS = dataStream.map(new NewsFeedSlidingMapper());

    DataStream<NewsFeed> timestampsAndWatermarksDS =
      selectDS.assignTimestampsAndWatermarks(new SessionAscendingTimestampAndWatermarkAssigner());

    KeyedStream<NewsFeed, Tuple2<String, String>> keyedDS = timestampsAndWatermarksDS
      .keyBy(
        new KeySelector<NewsFeed, Tuple2<String, String>>() {
          @Override
          public Tuple2<String, String> getKey(NewsFeed newsFeed) throws Exception {
            return new Tuple2<>(newsFeed.getSection(), newsFeed.getSubSection());
          }
        });

    WindowedStream<NewsFeed, Tuple2<String, String>, TimeWindow> windowedStream = keyedDS
      // Custom Window Assigner
      .window(SlidingNewsFlinkEventTimeWindow.of(Time.seconds(10), Time.seconds(3)))
      // Custom Trigger
      .trigger(NewsCountEventTimeOutTrigger.of(3));

    DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result =
      windowedStream.apply(new ApplyCustomWindowFunction());

    result.print();

    execEnv.execute("Event Time Session Window Apply");
  }

  private static class SessionAscendingTimestampAndWatermarkAssigner
    extends AscendingTimestampExtractor<NewsFeed> {
    @Override
    public long extractAscendingTimestamp(NewsFeed element) {
      return DateTimeFormat.forPattern("yyyyMMddHHmmss")
        .parseDateTime(element.getStartTimeStamp()).getMillis();
    }
  }

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    CustomWindowAssignerExample window = new CustomWindowAssignerExample();
    window.executeJob(parameterTool);
  }

}
