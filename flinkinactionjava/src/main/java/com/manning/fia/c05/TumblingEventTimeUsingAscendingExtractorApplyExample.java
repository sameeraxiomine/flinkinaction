package com.manning.fia.c05;

import com.manning.fia.model.media.BaseNewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper10;
import com.manning.fia.transformations.media.NewsFeedMapper3;
import com.manning.fia.utils.DataSourceFactory;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.joda.time.format.DateTimeFormat;

import java.util.List;

/**
 * Created by hari on 6/26/16.
 * expain the difference in line of using Assigner ..
 * while writing in  the book jst flip assignTimestampsAndWatermarks  with TumblingEventTimeUsingApplyExample
 */
public class TumblingEventTimeUsingAscendingExtractorApplyExample {

    public void executeJob(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment execEnv;
        DataStream<String> dataStream;
        DataStream<Tuple5<Long, String, String, String, String>> selectDS;
        DataStream<Tuple5<Long, String, String, String, String>> timestampsAndWatermarksDS;
        KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS;
        WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream;
        DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result;

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        execEnv.registerType(BaseNewsFeed.class);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

        selectDS = dataStream.map(new NewsFeedMapper3());

        timestampsAndWatermarksDS = selectDS.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());
        //timestampsAndWatermarksDS = selectDS.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssignerAsc());

        keyedDS = timestampsAndWatermarksDS.keyBy(1, 2);

        windowedStream = keyedDS.timeWindow(Time.seconds(10));

        result = windowedStream.apply(new ApplyFunction());

        result.print();
        execEnv.execute("Tumbling Event Time Window Using Ascending Apply");

    }

    private static class TimestampAndWatermarkAssignerAsc extends AscendingTimestampExtractor<Tuple5<Long, String, String, String, String>>{   	 
        @Override
        public long extractAscendingTimestamp(Tuple5<Long, String, String, String, String> element) {
            long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .parseDateTime(element.f3).getMillis();
            return millis;
        }
    }
    private static class TimestampAndWatermarkAssigner implements AssignerWithPeriodicWatermarks<Tuple5<Long, String, String, String, String>>{
   	 private long maxTimestamp=0;

		@Override
		public long extractTimestamp(Tuple5<Long, String, String, String, String> element,
		      long previousElementTimestamp) {
         long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
               .parseDateTime(element.f3).getMillis();
       this.maxTimestamp = Math.max(millis, maxTimestamp);
       return millis;		}

		@Override
		public Watermark getCurrentWatermark() {		
			return new Watermark(this.maxTimestamp);
		}
   }

   
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        TumblingEventTimeUsingAscendingExtractorApplyExample window = new TumblingEventTimeUsingAscendingExtractorApplyExample();
        window.executeJob(parameterTool);
    }
}
