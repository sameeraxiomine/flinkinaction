package com.manning.fia.c04;

/**
 * * * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed_for_count_windows
 */

import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.utils.NewsFeedDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class TumblingCountWindowExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();

        execEnv.setParallelism(parameterTool.getInt("parallelism", 1));


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

        WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream = keyedDS
                .countWindow(3);

        DataStream<Tuple3<String, String, Long>> result = windowedStream.sum(2);

        result.print();

        execEnv.execute("Tumbling Count Window");

    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        TumblingCountWindowExample window = new TumblingCountWindowExample();
        window.executeJob(parameterTool);

    }
}
