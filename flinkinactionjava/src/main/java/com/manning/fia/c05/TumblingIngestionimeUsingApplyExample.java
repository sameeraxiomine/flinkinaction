package com.manning.fia.c05;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper3;
import com.manning.fia.utils.DataSourceFactory;
import com.manning.fia.utils.custom.NewsFeedCustomDataSourceEmittingWM;
import com.manning.fia.utils.custom.NewsFeedCustomParallelDataSource;
import com.manning.fia.utils.custom.NewsFeedRichParallelSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.List;
import java.util.Map;

/**
 * Created by hari on 6/26/16.
 * --fileName /media/pipe/newsfeedparallelsource --threadSleepInterval 10
 *
 */
public class TumblingIngestionimeUsingApplyExample {

    public void executeJob(ParameterTool parameterTool) throws Exception {
        DataStream<String> eventStream;
        StreamExecutionEnvironment execEnv;
        int parallelism = parameterTool.getInt("parallelism", 1);
        KeyedStream<Tuple5<Long, String, String, String, String>, Tuple> keyedDS;
        DataStream<Tuple3<List<Long>,String,Long>> projectedDataStream;
        KeyedStream<Tuple3<List<Long>,String,Long>, Tuple> sectionKeyedDS;
        WindowedStream<Tuple5<Long, String, String, String, String>, Tuple, TimeWindow> windowedStream;
        WindowedStream<Tuple3<List<Long>,String,Long>, Tuple, TimeWindow> sectionWindowedStream;
        DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result;
        DataStream<Tuple5<Long, Long, List<Long>,  String, Long>> sectionResult;

        execEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        eventStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));
        keyedDS = eventStream.map(new NewsFeedMapper3()).keyBy(1,2);
        windowedStream = keyedDS.timeWindow(Time.seconds(5));
        result = windowedStream.apply(new ApplyFunction());
        result.print();
        
        projectedDataStream=result.project(2,3,5);
        sectionKeyedDS = projectedDataStream.keyBy(1);
        sectionWindowedStream=sectionKeyedDS.timeWindow((Time.seconds(5)));        
        sectionResult=sectionWindowedStream.apply(new SectionAggregator());        
        sectionResult.print();

        execEnv.execute("NewsFlink Ingestion Time Pipeline");
    }


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        TumblingIngestionimeUsingApplyExample window = new TumblingIngestionimeUsingApplyExample();
        window.executeJob(parameterTool);

    }



}
