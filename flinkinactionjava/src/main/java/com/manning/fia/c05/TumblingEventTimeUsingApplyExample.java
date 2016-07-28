package com.manning.fia.c05;

import com.manning.fia.utils.NewsFeedSocket;
import com.manning.fia.transformations.media.NewsFeedMapper3;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.joda.time.format.DateTimeFormat;

import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class TumblingEventTimeUsingApplyExample {

    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        System.out.println("getAutoWatermarkInterval" + execEnv.getConfig().getAutoWatermarkInterval());
        DataStream<String> socketStream = execEnv.socketTextStream("localhost",
                9000);

        DataStream<Tuple5<Long, String, String, String, String>> selectDS = socketStream
                .map(new NewsFeedMapper3());

        DataStream<Tuple5<Long, String, String, String, String>> timestampsAndWatermarksDS = selectDS
                .assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

        KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS = timestampsAndWatermarksDS
                .keyBy(1, 2);

        WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream = keyedDS
                .timeWindow(Time.seconds(10));

        DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result = windowedStream
                .apply(new ApplyFunction());

        result.print();
        execEnv.execute("Tumbling Event Time Window Apply");

    }

    private static class TimestampAndWatermarkAssigner
            implements
            AssignerWithPeriodicWatermarks<Tuple5<Long, String, String, String, String>> {
        private static final long serialVersionUID = 1L;
        private long wmTime = 0;
        private long priorWmTime = 0;
        private long lastTimeOfWaterMarking = System.currentTimeMillis();

        @Override
        public Watermark getCurrentWatermark() {
            if (wmTime == priorWmTime) {
                long advance = (System.currentTimeMillis() - lastTimeOfWaterMarking);
                wmTime += advance;// Start advancing
            }
            priorWmTime = wmTime;
            lastTimeOfWaterMarking = System.currentTimeMillis();
            return new Watermark(wmTime);
        }

        @Override
        public long extractTimestamp(
                Tuple5<Long, String, String, String, String> element,
                long previousElementTimestamp) {
            long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .parseDateTime(element.f3).getMillis();
            wmTime = Math.max(wmTime, millis);
            return Long.valueOf(millis);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Program Started at : "
                + DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
                        .print(System.currentTimeMillis()));
        new NewsFeedSocket("/media/pipe/newsfeed", 0,9000).start();
        TumblingEventTimeUsingApplyExample window = new TumblingEventTimeUsingApplyExample();
        window.executeJob();
    }
}
