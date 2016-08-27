package com.manning.fia.c05;

import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.utils.custom.NewsFeedCustomDataSourceEmittingWM;

/**
 * Created by hari on 6/26/16.
 * --fileName /media/pipe/newsfeedparallelsource --threadSleepInterval 10
 *
 */
public class TumblingEventTimeForDataSourceEmittingWM {

    public void executeJob(ParameterTool parameterTool) throws Exception {
        final DataStream<NewsFeed> eventStream;
        final StreamExecutionEnvironment execEnv;
        final int parallelism = parameterTool.getInt("parallelism", 1);
        final KeyedStream<NewsFeed, Tuple> keyedDS;
        final WindowedStream<NewsFeed, Tuple, TimeWindow> windowedStream;
        final DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result;

        execEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        eventStream = execEnv.addSource(new NewsFeedCustomDataSourceEmittingWM(parameterTool));
        keyedDS = eventStream.keyBy("section","subSection");
        windowedStream = keyedDS.timeWindow(Time.seconds(5));

        result = windowedStream.apply(new ApplyFunctionWithDomainObject());


        result.print();

        execEnv.execute("NewsFlink Event For DataSourceEmitting Water Mark");
    }


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        TumblingEventTimeForDataSourceEmittingWM window = new TumblingEventTimeForDataSourceEmittingWM();
        window.executeJob(parameterTool);

    }



}
