package com.manning.fia.c05;

import com.manning.fia.model.media.BaseNewsFeed;
import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.model.media.WaterMarksNewsFeed1;
import com.manning.fia.transformations.media.NewsFeedMapper10;
import com.manning.fia.utils.DataSourceFactory;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.joda.time.format.DateTimeFormat;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class TumblingEventTimeUsingUsingGeneratedWatermarkApply1Example {

    public void executeJob(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment execEnv;
        DataStream<String> dataStream;
        DataStream<BaseNewsFeed> selectDS;
        DataStream<BaseNewsFeed> timestampsAndWatermarksDS;
        KeyedStream<BaseNewsFeed, Tuple2<String, String>> keyedDS;
        WindowedStream<BaseNewsFeed, Tuple2<String,String>, TimeWindow> windowedStream;
        DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result;


        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

        selectDS = dataStream.map(new NewsFeedMapper10());

        timestampsAndWatermarksDS = selectDS.assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

        keyedDS = timestampsAndWatermarksDS.keyBy(new KeySelector<BaseNewsFeed, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, String> getKey(BaseNewsFeed baseNewsFeed) throws Exception {
               if (baseNewsFeed instanceof NewsFeed) {
                   NewsFeed newsFeed = (NewsFeed) baseNewsFeed;
                   return new Tuple2<>(newsFeed.getSection(), newsFeed.getSubSection());
               }
               return new Tuple2<>("","");
            }
        });

        windowedStream = keyedDS.timeWindow(Time.seconds(10));

        result = windowedStream.apply(new ApplyFunction3());

        result.print();

        execEnv.execute("Tumbling Event Time Window Using Watermark Apply");

    }

    private static class TimestampAndWatermarkAssigner implements
            AssignerWithPunctuatedWatermarks<BaseNewsFeed> {


        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(BaseNewsFeed newsFeed, long l) {
            return newsFeed instanceof WaterMarksNewsFeed1 ? new Watermark(l) : null;
        }

        @Override
        public long extractTimestamp(BaseNewsFeed newsFeed, long l) {
            long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(newsFeed.getStartTimeStamp()).getMillis();
            return millis;
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        TumblingEventTimeUsingUsingGeneratedWatermarkApply1Example window = new TumblingEventTimeUsingUsingGeneratedWatermarkApply1Example();
        window.executeJob(parameterTool);
    }
}
