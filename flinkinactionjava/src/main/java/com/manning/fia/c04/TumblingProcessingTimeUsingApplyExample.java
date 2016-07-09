package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper3;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.shaded.com.google.common.base.Throwables;
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
 */
public class TumblingProcessingTimeUsingApplyExample {

    public void executeJob() throws Exception{
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                   .createLocalEnvironment(1);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<String> socketStream = execEnv.socketTextStream(
                  "localhost", 9000);

        DataStream<Tuple5<Long, String, String, String, String>> selectDS = socketStream
                 .map(new NewsFeedMapper3());

        KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS = selectDS
                .keyBy(1, 2);

        WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream = keyedDS
                .timeWindow(Time.seconds(4));

        DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result = windowedStream
                .apply(new ApplyFunction());

        result.print();

        execEnv.execute("Processing Time Window Apply");
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed",3000,9000).start();
        TumblingProcessingTimeUsingApplyExample window = new TumblingProcessingTimeUsingApplyExample();
        window.executeJob();
    }
}
