package com.manning.fia.c04;

import com.manning.fia.model.media.ApplicationUser;
import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper5;
import com.manning.fia.transformations.media.NewsFeedMapper7;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

import java.util.concurrent.TimeUnit;

/**
 * Created by hari on 7/05/16.
 */
public class JoinedStreamsExample {

    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        execEnv.registerType(NewsFeed.class);


        DataStream<String> dataStream = execEnv.socketTextStream(
                "localhost", 9000);


        DataStream<Tuple7<Long, Long, String, String, String, Long, Long>> sectionSubsectionDS = dataStream
                .map(new NewsFeedMapper5());

        DataStream<Tuple7<Long, Long, String, String, String, Long, Long>> timestampsAndWatermarksDS =
                sectionSubsectionDS.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

        DataStream<Tuple7<Long, String, String[], String, ApplicationUser, Long, Long>> topicDs = dataStream.map(new NewsFeedMapper7());

        DataStream<Tuple7<Long, String, String[], String, ApplicationUser, Long, Long>>
                timestampsAndWatermarksDS1 = topicDs.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssignerForTopic());

        timestampsAndWatermarksDS.join(timestampsAndWatermarksDS1)
                .where(new Tuple7SectionSubsectionKeySelector())
                .equalTo(new Tuple7TopicKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(100)))
                .apply(new Tuple7Tuple7JoinFunction())
                .print()

        ;

        execEnv.execute("Joined Streams Example");


    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed5").start();
        JoinedStreamsExample window = new JoinedStreamsExample();
        window.executeJob();
    }


    private static class TimestampAndWatermarkAssigner
            implements
            AssignerWithPeriodicWatermarks<Tuple7<Long, Long, String, String, String, Long, Long>> {
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
                Tuple7<Long, Long, String, String, String, Long, Long> element,
                long previousElementTimestamp) {
            long millis = element.f5;
            wmTime = Math.max(wmTime, millis);
            return Long.valueOf(millis);
        }
    }

    private static class TimestampAndWatermarkAssignerForTopic
            implements
            AssignerWithPeriodicWatermarks<Tuple7<Long, String, String[], String,
                    ApplicationUser, Long, Long>> {
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
                Tuple7<Long, String, String[], String, ApplicationUser, Long, Long> element,
                long previousElementTimestamp) {
            long millis = element.f5;
            wmTime = Math.max(wmTime, millis);
            return Long.valueOf(millis);
        }
    }


    private static class Tuple7SectionSubsectionKeySelector implements KeySelector<Tuple7<Long, Long, String, String,
            String, Long, Long>, Long> {
        @Override
        public Long getKey(Tuple7<Long, Long, String, String, String, Long, Long> value) throws Exception {
            return value.f0;
        }
    }

    private static class Tuple7TopicKeySelector implements KeySelector<Tuple7<Long, String, String[], String,
            ApplicationUser, Long, Long>, Long> {
        @Override
        public Long getKey(Tuple7<Long, String, String[], String, ApplicationUser, Long, Long> value) throws Exception {
            return value.f0;
        }
    }

    private static class Tuple7Tuple7JoinFunction implements JoinFunction<Tuple7<Long, Long, String, String, String, Long, Long>, Tuple7<Long, String, String[], String, ApplicationUser, Long, Long>, Object> {
        @Override
        public NewsFeed join(Tuple7<Long, Long, String, String, String, Long, Long> first, Tuple7<Long, String,
                String[], String, ApplicationUser, Long, Long> second) throws Exception {
            return new NewsFeed(
                    first.f0, first.f1, first.f2, first.f3, first.f4, second.f1,
                    second.f2,
                    DateTimeFormat.forPattern("yyyyMMddHHmmss").print(second.f5),
                    DateTimeFormat.forPattern("yyyyMMddHHmmss").print(second.f6),
                    second.f3, second.f4);
        }
    }
}
