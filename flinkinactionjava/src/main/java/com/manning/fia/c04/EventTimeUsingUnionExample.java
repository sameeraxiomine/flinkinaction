package com.manning.fia.c04;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper3;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
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

import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class EventTimeUsingUnionExample {

    public void executeJob() {
        try {
            StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                    .createLocalEnvironment(1);

            execEnv.registerType(NewsFeed.class);

            execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStream<String> socketStream = execEnv.socketTextStream(
                    "localhost", 9000);

            DataStream<String> secondSocketStream = execEnv.socketTextStream(
                    "localhost", 8000);


            DataStream<String> unionSocketStream = socketStream.union(secondSocketStream);

            DataStream<Tuple5<Long, String, String, String, String>> selectDS = unionSocketStream
                    .map(new NewsFeedMapper3());

            unionSocketStream.print();

            DataStream<Tuple5<Long, String, String, String, String>> timestampsAndWatermarksDS = selectDS
                    .assignTimestampsAndWatermarks(new NewsFeedTimeStamp());

            KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS = timestampsAndWatermarksDS
                    .keyBy(1, 2);


            WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream = keyedDS
                    .timeWindow(Time.seconds(2));


            DataStream<Tuple8<String, String, String, String, Long, Long, Long, List<Long>>> result = windowedStream
                    .apply(new ApplyFunction());

            result.print();

            execEnv.execute("Event Time Union Window Apply");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    private static class NewsFeedTimeStamp implements AssignerWithPeriodicWatermarks<Tuple5<Long, String, String, String, String>> {
        private static final long serialVersionUID = 1L;

        private long maxTimestamp;


        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(Long.MIN_VALUE);
        }

        @Override
        public long extractTimestamp(Tuple5<Long, String, String, String, String> element, long previousElementTimestamp) {
            maxTimestamp = Math.max(maxTimestamp, Long.valueOf(element.f3));
            return Long.valueOf(element.f3);
        }
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
        new NewsFeedSocket("/media/pipe/newsfeed2",8000).start();

        EventTimeUsingUnionExample window = new EventTimeUsingUnionExample();
        window.executeJob();

    }
}
