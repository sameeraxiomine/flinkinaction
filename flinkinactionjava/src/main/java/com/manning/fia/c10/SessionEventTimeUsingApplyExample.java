package com.manning.fia.c10;

import com.manning.fia.utils.DataSourceFactory;
import java.util.List;
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

// Run with newsfeed_for_session_windows and threadsleepinterval = 500ms
public class SessionEventTimeUsingApplyExample {
  private void executeJob(ParameterTool parameterTool) throws Exception{
    StreamExecutionEnvironment execEnv;
    DataStream<Tuple6<String, Long, String, String, String, String>> selectDS;
    DataStream<Tuple6<String, Long, String, String, String, String>> timestampsAndWatermarksDS;
    KeyedStream<Tuple6<String, Long, String, String, String, String>, Tuple> keyedDS;
    WindowedStream<Tuple6<String, Long, String, String, String, String>, Tuple, TimeWindow> windowedStream;
    DataStream<Tuple5<Long, Long, String, List<Long>, Long>> result;

    execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    execEnv.setParallelism(parameterTool.getInt("parallelism", execEnv.getParallelism()));

    execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    final DataStream<String> dataStream;

    dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

    selectDS = dataStream.map(new NewsFeedSubscriberMapper());

    timestampsAndWatermarksDS = selectDS.assignTimestampsAndWatermarks(new NewsFeedTimeStamp());

    keyedDS = timestampsAndWatermarksDS.keyBy(0);

    windowedStream = keyedDS.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

    result = windowedStream.apply(new ApplySessionWindowFunction());

    result.print();

    execEnv.execute("Event Time Session Window Apply");
  }

  private static class NewsFeedTimeStamp implements
    AssignerWithPeriodicWatermarks<Tuple6<String, Long, String, String, String, String>> {
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
      Tuple6<String, Long, String, String, String, String> element,
        long previousElementTimestamp) {
      long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
          .parseDateTime(element.f4).getMillis();
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
