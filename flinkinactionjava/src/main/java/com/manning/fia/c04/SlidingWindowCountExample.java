package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * Created by hari on 5/30/16.
 */
public class SlidingWindowCountExample {

    public void executeJob() {
        try {
            StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                    .createLocalEnvironment(1);

            DataStream<String> socketStream = execEnv.socketTextStream(
                    "localhost", 9000);

            DataStream<Tuple3<String, String, Long>> selectDS = socketStream
                    .map(new NewsFeedMapper()).project(1, 2, 4);

            KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS = selectDS
                    .keyBy(1, 2);

            WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream = keyedDS
                    .countWindow(4, 1);

            DataStream<Tuple3<String, String, Long>> result = windowedStream.sum(2);

            result.print();

            execEnv.execute("Sliding Count Window");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
        SlidingWindowCountExample window = new SlidingWindowCountExample();
        window.executeJob();

    }
}

