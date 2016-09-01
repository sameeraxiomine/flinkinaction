package com.manning.fia.c06;

import com.manning.fia.c04.DataStreamGenerator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Created by hari on 5/30/16.
 * <p>
 * * * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed3 --threadSleepInterval 1000
 */
public class SlidingWindowAllExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        StreamExecutionEnvironment execEnv;
        DataStream<Tuple3<String, String, Long>> selectDS;
        AllWindowedStream<Tuple3<String, String, Long>, TimeWindow> windowedStream;
        DataStream<Tuple3<String, String, Long>> result;

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        selectDS = DataStreamGenerator.getC04ProjectedDataStream(execEnv, parameterTool);

        windowedStream = selectDS.timeWindowAll(Time.seconds(25), Time.seconds(5));

        // Above code and the following  one are same.
//        AllWindowedStream<Tuple3<String, String, Long>, TimeWindow> windowedStream = selectDS
//                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(25),Time.seconds(5)));

        result = windowedStream.sum(2);

        result.project(2).print();

        execEnv.execute("Tumbling Time Window");

    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SlidingWindowAllExample window = new SlidingWindowAllExample();
        window.executeJob(parameterTool);

    }
}
