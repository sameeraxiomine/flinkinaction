package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * Created by hari on 5/30/16.
 *
 * * * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed_for_count_windows
 */

public class SlidingCountWindowExample {

    public void executeJob(ParameterTool parameterTool) throws Exception {

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);
        execEnv.setParallelism(parameterTool.getInt("parallelism", execEnv.getParallelism()));
        final DataStream<String> dataStream;
        boolean isKafka = parameterTool.getBoolean("isKafka", false);
        if (isKafka) {
            dataStream = execEnv.addSource(NewsFeedDataSource.getKafkaDataSource(parameterTool));
        } else {
            dataStream = execEnv.addSource(NewsFeedDataSource.getCustomDataSource(parameterTool));
        }

        DataStream<Tuple3<String, String, Long>> selectDS = dataStream.map(
                new NewsFeedMapper()).project(1, 2, 4);


        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS = selectDS
                .keyBy(0, 1);
        int size = 3;
        int slide = 2;
        WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream = keyedDS
                .countWindow(size, slide);

        DataStream<Tuple3<String, String, Long>> result = windowedStream.sum(2);

        result.print();
        execEnv.execute("Sliding Count Window");

    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SlidingCountWindowExample window = new SlidingCountWindowExample();
        window.executeJob(parameterTool);
    }
}
