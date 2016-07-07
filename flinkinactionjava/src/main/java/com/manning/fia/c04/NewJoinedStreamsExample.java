package com.manning.fia.c04;

import com.manning.fia.model.media.ApplicationUser;
import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedJoinedMapper5;
import com.manning.fia.transformations.media.NewsFeedMapper5;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.format.DateTimeFormat;

/**
 * Created by hari on 7/05/16.
 */
public class NewJoinedStreamsExample {

    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        execEnv.registerType(NewsFeed.class);


        DataStream<String> dataStream = execEnv.socketTextStream(
                "localhost", 9000);

        DataStream<String> dataStream1 = execEnv.socketTextStream(
                "localhost", 8000);




        DataStream<Tuple6<Long, String, String, String, Long, Long>> sectionSubsectionDS = dataStream
                .map(new NewsFeedJoinedMapper5());

        DataStream<Tuple6<Long, String, String, String, Long, Long>> timestampsAndWatermarksDS =
                sectionSubsectionDS.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

        DataStream<Tuple6<Long, String, String, String, Long, Long>> topicDs = dataStream1
                .map(new NewsFeedJoinedMapper5());


        DataStream<Tuple6<Long, String, String, String, Long, Long>> timestampsAndWatermarksDS1 =
                topicDs.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());


        timestampsAndWatermarksDS.join(timestampsAndWatermarksDS1)
                .where(new Tuple6KeySelector())
                .equalTo(new Tuple6KeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(200)))
                .apply(new JoinFunction<Tuple6<Long,String,String,String,Long,Long>, Tuple6<Long,String,String,String,Long,Long>, Object>() {
                    @Override
                    public Object join(Tuple6<Long, String, String, String, Long, Long> first,
                                       Tuple6<Long, String, String, String, Long, Long> second) throws Exception {
                       return Long.parseLong(DateTimeFormat.forPattern("yyyyMMddHHmmss").print(first.f4))+
                        "|"+ Long.parseLong(DateTimeFormat.forPattern("yyyyMMddHHmmss").print(second.f4));
                    }
                })
                .print()

        ;

        execEnv.execute("Joined Streams Example");


    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed5",1000,9000).start();
        new NewsFeedSocket("/media/pipe/newsfeed6", 1000, 8000).start();
        NewJoinedStreamsExample window = new NewJoinedStreamsExample();
        window.executeJob();
    }

    private static class WaterMarkAssigner
            implements
            AssignerWithPeriodicWatermarks<NewsFeed> {
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
                NewsFeed element,
                long previousElementTimestamp) {
            long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .parseDateTime(element.getStartTimeStamp())
                    .getMillis();
            wmTime = Math.max(wmTime, millis);
            return Long.valueOf(millis);
        }
    }


    private static class TimestampAndWatermarkAssigner
            implements
            AssignerWithPeriodicWatermarks<Tuple6<Long, String, String, String, Long, Long>> {
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
                Tuple6<Long, String, String, String, Long, Long> element,
                long previousElementTimestamp) {
            long millis = element.f5;
            wmTime = Math.max(wmTime, millis);
            return Long.valueOf(millis);
        }
    }


    private static class Tuple6KeySelector implements KeySelector<Tuple6<Long, String, String, String, Long, Long>, String> {
        @Override
        public String getKey(Tuple6<Long, String, String, String, Long, Long> value) throws Exception {
            return value.f2 + "|" + value.f3;
        }
    }

    private static class Tuple7TopicKeySelector implements KeySelector<Tuple7<Long, String, String[], String,
            ApplicationUser, Long, Long>, Long> {
        @Override
        public Long getKey(Tuple7<Long, String, String[], String, ApplicationUser, Long, Long> value) throws Exception {
            return value.f0;
        }
    }


}
