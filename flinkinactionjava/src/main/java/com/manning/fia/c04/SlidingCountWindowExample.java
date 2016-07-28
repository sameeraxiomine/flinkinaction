package com.manning.fia.c04;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * Created by hari on 5/30/16.
 * <p>
 * * * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed_for_count_windows
 */

public class SlidingCountWindowExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        StreamExecutionEnvironment execEnv;
        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS;
        WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream;
        DataStream<Tuple3<String, String, Long>> result;
        int size = 3;
        int slide = 2;

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        keyedDS = DataStreamGenerator.getC04KeyedStream(execEnv, parameterTool);

        windowedStream = keyedDS.countWindow(size, slide);

        result = windowedStream.sum(2);

        result.print();

        execEnv.execute("Sliding Count Window");

    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SlidingCountWindowExample window = new SlidingCountWindowExample();
        window.executeJob(parameterTool);
    }
}
