package com.manning.fia.c04;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper3;

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
                    .timeWindow(Time.seconds(20));


            DataStream<Tuple6<Long,Long, List<Long>,String, String, Long >> result = windowedStream
                    .apply(new ApplyFunction());

            result.print();

            execEnv.execute("Event Time Window Apply");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    private static class NewsFeedTimeStamp2 extends AscendingTimestampExtractor<Tuple5<Long, String, String, String, String>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Tuple5<Long, String, String, String, String> element) {
            
            return Long.valueOf(DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .parseDateTime(element.f3)
                    .getMillis());
        }
    }
    
    private static class NewsFeedTimeStamp implements AssignerWithPeriodicWatermarks<Tuple5<Long, String, String, String, String>> {
        private static final long serialVersionUID = 1L;
        public static int DELAY=5000;
        private long maxTimestamp=0;
        private long priorTimestamp=0;
        private long lastTimeOfWaterMarking=System.currentTimeMillis();
        @Override
        public Watermark getCurrentWatermark() {
            if(maxTimestamp==priorTimestamp){
                long advance = (System.currentTimeMillis()-lastTimeOfWaterMarking);
                maxTimestamp+=advance;//Start advancing                
            }
            priorTimestamp=maxTimestamp;
            lastTimeOfWaterMarking = System.currentTimeMillis();
            return new Watermark(maxTimestamp);
        }

        @Override
        public long extractTimestamp(Tuple5<Long, String, String, String, String> element, long previousElementTimestamp) {
            long millis= DateTimeFormat.forPattern("yyyyMMddHHmmss")
            .parseDateTime(element.f3).getMillis();
            maxTimestamp = Math.max(maxTimestamp, millis-DELAY);
            return Long.valueOf(millis);
        }
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
        EventTimeUsingApplyExample window = new EventTimeUsingApplyExample();
        window.executeJob();

    }
}
