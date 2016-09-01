package com.manning.fia.c06;

import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.utils.DataSourceFactory;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

// run with parallelism = 1 and threadSleepInterval = 1
public class GlobalWindowsCountWithCustomTrigger {

  private void executeJob(final ParameterTool parameters) throws Exception {
    StreamExecutionEnvironment execEnv;
    WindowedStream<Tuple3<String, String, Long>, Tuple2<String, String>, GlobalWindow> windowedStream;
    DataStream<Tuple3<String, String, Long>> result;

    execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameters));

    DataStream<Tuple3<String, String, Long>> selectDS = dataStream.map(new NewsFeedMapper()).project(1, 2, 4);

    KeyedStream<Tuple3<String, String, Long>, Tuple2<String, String>> keyedDS =
      selectDS.keyBy(new KeySelector<Tuple3<String, String, Long>, Tuple2<String, String>>() {
        @Override
        public Tuple2<String, String> getKey(Tuple3<String, String, Long> tuple3) throws Exception {
          return new Tuple2<>(tuple3.f0, tuple3.f1);
        }
      });

    windowedStream = keyedDS.window(GlobalWindows.create());

    windowedStream.trigger(PurgingTrigger.of(NewsCountTimeoutTrigger.of(3, 6000)));
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
