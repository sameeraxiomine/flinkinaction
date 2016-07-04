package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.concurrent.TimeUnit;

public class SlidingWindowAllExample {
    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);

        DataStream<String> socketStream = execEnv.socketTextStream("localhost",
                9000);

        DataStream<Tuple3<String, String, Long>> selectDS = socketStream.map(
                new NewsFeedMapper()).project(1, 2, 4);

        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS = selectDS
                .keyBy(0, 1);

        AllWindowedStream<Tuple3<String, String, Long>, TimeWindow> windowedStream = keyedDS
               .timeWindowAll(Time.seconds(25),Time.seconds(5));

        // Above code and the following  one are same.
//        AllWindowedStream<Tuple3<String, String, Long>, TimeWindow> windowedStream = keyedDS
//                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(25),Time.seconds(5)));

        DataStream<Tuple3<String, String, Long>> result = windowedStream.sum(2);

        result.project(2).print();

        execEnv.execute("Tumbling Time Window");

    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed3",1000,9000).start();
        SlidingWindowAllExample window = new SlidingWindowAllExample();
        window.executeJob();

    }
}
