package com.manning.fia.c04;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper3;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class EventTimeUsingApplyExample {

    public void executeJob() {
        try {
            StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                    .createLocalEnvironment(1);

            execEnv.registerType(NewsFeed.class);

            execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            DataStream<String> socketStream = execEnv.socketTextStream(
                    "localhost", 9000);

            DataStream<Tuple5<Long, String, String, String, String>> selectDS = socketStream
                    .map(new NewsFeedMapper3());

            DataStream<Tuple5<Long, String, String, String, String>> timestampsAndWatermarksDS = selectDS
                    .assignTimestampsAndWatermarks(new NewsFeedTimeStamp());

            KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS = timestampsAndWatermarksDS
                    .keyBy(1, 2);

            WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream = keyedDS
                    .timeWindow(Time.seconds(2), Time.seconds(1));


            DataStream<Tuple8<String, String, String, String, Long, Long, Long, List<Long>>> result = windowedStream
                    .apply(new ApplyFunction());

            result.print();

            execEnv.execute("Event Time Window Apply");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    private static class NewsFeedTimeStamp extends AscendingTimestampExtractor<Tuple5<Long, String, String, String, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Tuple5<Long, String, String, String, String> element) {
            return Long.valueOf(element.f3);
        }
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
        EventTimeUsingApplyExample window = new EventTimeUsingApplyExample();
        window.executeJob();

    }
}
