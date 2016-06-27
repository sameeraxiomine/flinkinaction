package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedMapper2;
import com.manning.fia.transformations.media.NewsFeedMapper3;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class WindowedStreamApplyExample {

    public void executeJob() {
        try {
            StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                    .createLocalEnvironment(1);

            DataStream<String> socketStream = execEnv.socketTextStream(
                    "localhost", 9000);

            DataStream<Tuple6<Long, String, String, Long, Long, Long>> selectDS = socketStream
                    .map(new NewsFeedMapper3());

            KeyedStream<Tuple6<Long, String, String, Long, Long, Long>, Tuple> keyedDS = selectDS
                    .keyBy(1, 2);

            WindowedStream<Tuple6<Long, String, String, Long, Long, Long>, Tuple, TimeWindow> windowedStream = keyedDS
                    .timeWindow(Time.milliseconds(50));

            DataStream<Tuple6<String, String, Long, Long, Long, List<Long>>> result = windowedStream
                    .apply(new ApplyFunction());

            result.print();

            execEnv.execute("Tumbling Time Window");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
        WindowedStreamApplyExample window = new WindowedStreamApplyExample();
        window.executeJob();

    }
}
