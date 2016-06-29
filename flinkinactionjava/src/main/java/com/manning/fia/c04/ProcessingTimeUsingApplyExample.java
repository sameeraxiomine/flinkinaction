package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper3;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class ProcessingTimeUsingApplyExample {

    public void executeJob() {
        try {
            StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                    .createLocalEnvironment(1);

            DataStream<String> socketStream = execEnv.socketTextStream(
                    "localhost", 9000);

            DataStream<Tuple5<Long, String, String, String, String>> selectDS = socketStream
                    .map(new NewsFeedMapper3());

            KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS = selectDS
                    .keyBy(1, 2);

            WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream = keyedDS
                    .timeWindow(Time.milliseconds(50));

            DataStream<Tuple8<String, String, String, String, Long, Long, Long, List<Long>>> result = windowedStream
                    .apply(new ApplyFunction());

            result.print();

            execEnv.execute("Processing Time Window Apply");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
        ProcessingTimeUsingApplyExample window = new ProcessingTimeUsingApplyExample();
        window.executeJob();

    }
}
