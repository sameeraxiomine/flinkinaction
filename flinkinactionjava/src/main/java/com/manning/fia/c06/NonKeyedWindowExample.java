package com.manning.fia.c06;

import com.manning.fia.c04.DataStreamGenerator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class NonKeyedWindowExample {

  private void executeJob(ParameterTool parameterTool) throws Exception {

    StreamExecutionEnvironment execEnv;
    DataStream<Tuple3<String, String, Long>> selectDS;
    AllWindowedStream<Tuple3<String, String, Long>, TimeWindow> windowedStream;
    DataStream<Tuple3<String, String, Long>> result;

    execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    selectDS = DataStreamGenerator.getC04ProjectedDataStream(execEnv, parameterTool);

//    windowedStream = selectDS.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(25),Time.seconds(5)));
    windowedStream = selectDS.timeWindowAll(Time.seconds(25), Time.seconds(5));

    result = windowedStream.sum(2);

    result.project(2).print();

    execEnv.execute("Tumbling Time Window All");

  }

  public static void main(String[] args) throws Exception {
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    NonKeyedWindowExample window = new NonKeyedWindowExample();
    window.executeJob(parameterTool);
  }
}
