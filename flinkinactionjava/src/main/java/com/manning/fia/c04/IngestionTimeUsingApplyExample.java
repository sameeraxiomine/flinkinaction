package com.manning.fia.c04;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper3;
import com.manning.fia.transformations.media.NewsFeedMapper4;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.joda.time.format.DateTimeFormat;

import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class IngestionTimeUsingApplyExample {
    public static String HOST = "localhost";
    public static int PORT = 6123;
    public static String JAR_PATH = "target/flinkinactionjava-0.0.1-SNAPSHOT-jar-with-dependencies.jar";
    public static int DEFAULT_LOCAL_PARALLELISM = 1;
    public static int DEFAULT_REMOTE_PARALLELISM = 5;
    public void executeJob() {
        try {
            StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(4);

            execEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

            DataStream<String> socketStream = execEnv.socketTextStream(
                    "localhost", 9000);

            DataStream<Tuple5<Long, String, String, String, String>> selectDS1 = socketStream
                    .map(new NewsFeedMapper3());
            DataStream<Tuple4<Long, String, String, String>> selectDS2 = socketStream
                    .map(new NewsFeedMapper4());



            KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS1 = selectDS1.keyBy(1, 2);
            KeyedStream<Tuple4<Long, String, String, String>, Tuple> keyedDS2 = selectDS2.keyBy(1);

            WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream1 = keyedDS1
                    .timeWindow(Time.seconds(4));
            WindowedStream<Tuple4<Long, String, String, String>, Tuple, TimeWindow> windowedStream2 = keyedDS2
                    .timeWindow(Time.seconds(4));


            DataStream<Tuple6<Long,Long, List<Long>,String, String, Long >> result1 = windowedStream1
                    .apply(new ApplyFunction());

            DataStream<Tuple5<Long,Long, List<Long>,String, Long >> result2 = windowedStream2
                    .apply(new ApplyFunction2());

            result1.print();
            result2.print();

            execEnv.execute("Ingestion Time Window Apply");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }


    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
        IngestionTimeUsingApplyExample window = new IngestionTimeUsingApplyExample();
        window.executeJob();

    }
}
