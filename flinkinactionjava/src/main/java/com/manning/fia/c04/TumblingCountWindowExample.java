package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TumblingCountWindowExample {

    public void executeJob() throws Exception {

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);

        DataStream<String> socketStream = execEnv.socketTextStream("localhost",
                9000);

        DataStream<Tuple3<String, String, Long>> selectDS = socketStream.map(
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
        new NewsFeedSocket("/media/pipe/newsfeed_for_count_windows").start();

        TumblingCountWindowExample window = new TumblingCountWindowExample();
        window.executeJob();

    }
}
