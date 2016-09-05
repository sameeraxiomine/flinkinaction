package com.manning.fia.c06;

import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.utils.DataSourceFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

// run with parallelism = 1 and threadSleepInterval = 3000ms, filename = newsfeed3
public class GlobalWindowsCountWithCustomTrigger {

  private void executeJob(final ParameterTool parameters) throws Exception {
    StreamExecutionEnvironment execEnv;
    DataStream<String> dataStream;
    WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream;
    DataStream<Tuple3<String, String, Long>> result;
    DataStream<Tuple3<String, String, Long>> selectDS;

    execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameters));

    selectDS = dataStream.map(new NewsFeedMapper()).project(1, 2, 4);

    KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS = selectDS.keyBy(0, 1);

    windowedStream = keyedDS.window(GlobalWindows.create());

    windowedStream.trigger(PurgingTrigger.of(NewsCountTimeoutTrigger.of(3, 3000)));
    result = windowedStream.sum(2);

    result.print();

    execEnv.execute("Global Windows with Custom Trigger");
  }

  public static void main(final String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    GlobalWindowsCountWithCustomTrigger window = new GlobalWindowsCountWithCustomTrigger();
    window.executeJob(parameterTool);

  }
}
