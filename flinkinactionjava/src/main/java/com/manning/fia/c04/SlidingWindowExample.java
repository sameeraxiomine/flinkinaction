package com.manning.fia.c04;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class SlidingWindowExample {


    private void executeJob(ParameterTool parameterTool) throws Exception {

        StreamExecutionEnvironment execEnv;
        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS;
        WindowedStream<Tuple3<String, String, Long>, Tuple, TimeWindow> windowedStream;
        DataStream<Tuple3<String, String, Long>> result;

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        keyedDS = DataStreamGenerator.getC04KeyedStream(execEnv, parameterTool);

        windowedStream = keyedDS.timeWindow(Time.seconds(15), Time.seconds(5));

        result = windowedStream.sum(2);

        result.print();

        execEnv.execute("Sliding Windows");

    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SlidingWindowExample window = new SlidingWindowExample();
        window.executeJob(parameterTool);

    }
}
