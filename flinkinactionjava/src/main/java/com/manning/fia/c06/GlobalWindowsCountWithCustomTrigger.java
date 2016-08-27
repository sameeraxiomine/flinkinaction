package com.manning.fia.c06;

import com.manning.fia.c04.DataStreamGenerator;
import com.manning.fia.utils.NewsFeedSocket;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class GlobalWindowsCountWithCustomTrigger {

  private void executeJob(final ParameterTool parameters) throws Exception {
    StreamExecutionEnvironment execEnv;
    KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS;
    WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream;
    DataStream<Tuple3<String, String, Long>> result;

    execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    keyedDS = DataStreamGenerator.getC04KeyedStream(execEnv, parameters);

    windowedStream = keyedDS.window(GlobalWindows.create());

    windowedStream.trigger(NewsTimeoutTrigger.of(CountTrigger.of(3), 10));

    result = windowedStream.sum(2);

    result.print();

    execEnv.execute("Global Windows with Custom Trigger");
  }

  public static void main(final String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    new NewsFeedSocket("/media/pipe/newsfeed_for_count_windows").start();
    GlobalWindowsCountWithCustomTrigger window = new GlobalWindowsCountWithCustomTrigger();
    window.executeJob(parameterTool);

  }
}
