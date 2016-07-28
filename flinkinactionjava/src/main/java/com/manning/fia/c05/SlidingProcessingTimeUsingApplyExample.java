package com.manning.fia.c05;

import com.manning.fia.transformations.media.NewsFeedMapper3;
import com.manning.fia.utils.NewsFeedDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.List;

/**
 * Created by hari on 6/26/16.
 *  * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --threadSleepInterval 1000
 */

public class SlidingProcessingTimeUsingApplyExample {

    private void executeJob(ParameterTool parameterTool) throws Exception{
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        execEnv.setParallelism(parameterTool.getInt("parallelism", execEnv.getParallelism()));
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        final DataStream<String> dataStream;
        boolean isKafka = parameterTool.getBoolean("isKafka", false);
        if (isKafka) {
            dataStream = execEnv.addSource(NewsFeedDataSource.getKafkaDataSource(parameterTool));
        } else {
            dataStream = execEnv.addSource(NewsFeedDataSource.getCustomDataSource(parameterTool));
        }
        DataStream<Tuple5<Long, String, String, String, String>> selectDS = dataStream
                 .map(new NewsFeedMapper3());

        KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS = selectDS
                .keyBy(1, 2);

        WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream = keyedDS
                .timeWindow(Time.seconds(2),Time.seconds(1));

        DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result = windowedStream
                .apply(new ApplyFunction());

        result.print();

        execEnv.execute("Processing Time Window Apply");
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SlidingProcessingTimeUsingApplyExample window = new SlidingProcessingTimeUsingApplyExample();
        window.executeJob(parameterTool);
    }
}
