package com.manning.fia.c04;

import com.manning.fia.transformations.media.ComputeTimeSpentPerSectionAndSubSection;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * this example basically does a simple streaming i.e grouping the data keys and
 * doing a sum aggregation cummulatively as & when the data arrives. no concept
 * of Windows as it is basically KeyedStream
 * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed
 */
public class SimpleStreamingExample2 {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        StreamExecutionEnvironment execEnv;
        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS;
        WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream;
        DataStream<Tuple3<String, String, Long>> result;

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        keyedDS = DataStreamGenerator.getC04KeyedStream(execEnv, parameterTool);

        result = keyedDS.reduce(new ComputeTimeSpentPerSectionAndSubSection());

        result.print();

        execEnv.execute("Simple Streaming 2");
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimpleStreamingExample2 window = new SimpleStreamingExample2();
        window.executeJob(parameterTool);
    }
}
