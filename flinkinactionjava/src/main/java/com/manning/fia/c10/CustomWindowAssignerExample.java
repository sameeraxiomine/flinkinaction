package com.manning.fia.c10;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.utils.DataSourceFactory;
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
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.joda.time.format.DateTimeFormat;

public class CustomWindowAssignerExample {

  private void executeJob(final ParameterTool parameterTool) throws Exception{
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result;
    WindowedStream<NewsFeed, Tuple2<String, String>, TimeWindow> windowedStream;
    KeyedStream<NewsFeed, Tuple2<String, String>> keyedDS;
    final DataStream<String> dataStream;

    execEnv.setParallelism(parameterTool.getInt("parallelism", execEnv.getParallelism()));

    execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

    DataStream<NewsFeed> selectDS = dataStream.map(new NewsFeedSlidingMapper());

    DataStream<NewsFeed> timestampsAndWatermarksDS =
      selectDS.assignTimestampsAndWatermarks(new NewsFeedTimeStamp());

    keyedDS = timestampsAndWatermarksDS.keyBy(
        new KeySelector<NewsFeed, Tuple2<String, String>>() {
          @Override
          public Tuple2<String, String> getKey(NewsFeed newsFeed) throws Exception {
            return new Tuple2<>(newsFeed.getSection(), newsFeed.getSubSection());
          }
        });

    windowedStream = keyedDS.window(SlidingNewsFlinkEventTimeWindow.of(Time.seconds(3),
                                                                       Time.seconds(2)));

    result = windowedStream.apply(new ApplyCustomWindowFunction());

    result.print();

    execEnv.execute("Event Time Session Window Apply");
  }

  private static class NewsFeedTimeStamp
    implements AssignerWithPeriodicWatermarks<NewsFeed> {
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
    public long extractTimestamp(NewsFeed element, long previousElementTimestamp) {
      long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
        .parseDateTime(element.getStartTimeStamp()).getMillis();
      maxTimestamp = Math.max(maxTimestamp, millis);
      return millis;
    }
  }


  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
//    new NewsFeedSocket("/media/pipe/newsfeed_for_custom_sliding_windows").start();
    CustomWindowAssignerExample window = new CustomWindowAssignerExample();
    window.executeJob(parameterTool);
  }

}
