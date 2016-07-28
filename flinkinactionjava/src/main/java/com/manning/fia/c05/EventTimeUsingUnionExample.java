package com.manning.fia.c05;


import com.manning.fia.utils.NewsFeedSocket;
import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper3;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
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
public class EventTimeUsingUnionExample {

    public void executeJob() {
        try {
            StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                    .createLocalEnvironment(1);

            execEnv.registerType(NewsFeed.class);

            execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            
            execEnv.getConfig().setAutoWatermarkInterval(10000);            

            DataStream<Tuple5<Long, String, String, String, String>>  socketStream = execEnv.socketTextStream(
                    "localhost", 9000).map(new NewsFeedMapper3()).assignTimestampsAndWatermarks(new NewsFeedTimeStamp());


            DataStream<Tuple5<Long, String, String, String, String>>  secondSocketStream = execEnv.socketTextStream(
                    "localhost", 8000).map(new NewsFeedMapper3()).assignTimestampsAndWatermarks(new NewsFeedTimeStamp());


            DataStream<Tuple5<Long, String, String, String, String>> unionSocketStream = socketStream.union(secondSocketStream);

            
                    

            //unionSocketStream.print();


            KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS = unionSocketStream
                    .keyBy(1, 2);


            WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream = keyedDS
                    .timeWindow(Time.seconds(2));


            DataStream<Tuple6<Long, Long,List<Long>, String, String, Long>> result = windowedStream
                    .apply(new ApplyFunction());

            result.print();

            execEnv.execute("Event Time Union Window Apply");

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
        private final long maxOutOfOrderness = 0; 
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
            return new Watermark(maxTimestamp-maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple5<Long, String, String, String, String> element, long previousElementTimestamp) {
            long millis= DateTimeFormat.forPattern("yyyyMMddHHmmss")
            .parseDateTime(element.f3).getMillis();//Always delay watermarks by 5 seconds
            maxTimestamp = Math.max(maxTimestamp, millis);
            return Long.valueOf(millis);
        }
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed",1000,9000).start();
        new NewsFeedSocket("/media/pipe/newsfeed",4000,8000).start();

        EventTimeUsingUnionExample window = new EventTimeUsingUnionExample();
        window.executeJob();

    }
}
