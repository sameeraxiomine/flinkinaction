package com.manning.fia.c05;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.model.media.WaterMarkedNewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper11;
import com.manning.fia.utils.DataSourceFactory;
import org.apache.flink.api.java.tuple.Tuple;
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
public class TumblingEventTimeUsingUsingPunctuatedWatermarkExample {

    public void executeJob(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment execEnv;
        DataStream<String> dataStream;
        DataStream<NewsFeed> selectDS;
        DataStream<NewsFeed> timestampsAndWatermarksDS;
        KeyedStream<NewsFeed, Tuple> keyedDS;
        WindowedStream<NewsFeed, Tuple, TimeWindow> windowedStream;
        DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result;


        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        execEnv.registerType(NewsFeed.class);

        dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

        selectDS = dataStream.map(new NewsFeedMapper11());

        timestampsAndWatermarksDS = selectDS.assignTimestampsAndWatermarks(new TimestampAndPunctuatedWatermarkAssigner());

        keyedDS = timestampsAndWatermarksDS.keyBy("section","subSection");

        windowedStream = keyedDS.timeWindow(Time.seconds(15));

        result = windowedStream.apply(new ApplyFunctionWithDomainObject());

        result.print();

        execEnv.execute("Tumbling Event Time Window Using Watermark Apply");

    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        TumblingEventTimeUsingUsingPunctuatedWatermarkExample window = new TumblingEventTimeUsingUsingPunctuatedWatermarkExample();
        window.executeJob(parameterTool);
    }
}
