package com.manning.fia.c05;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.utils.custom.NewsFeedCustomParallelDataSource;
import com.manning.fia.utils.custom.NewsFeedRichParallelSource;
import org.apache.flink.api.java.tuple.Tuple;
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
        final Map<Integer, List<NewsFeed>> data;
        final DataStream<NewsFeed> eventStream;
        final StreamExecutionEnvironment execEnv;
        final int parallelism = parameterTool.getInt("parallelism", 5);
        final KeyedStream<NewsFeed, Tuple> keyedDS;
        final KeyedStream<NewsFeed, Tuple> sectionKeyedDS;
        final WindowedStream<NewsFeed, Tuple, TimeWindow> windowedStream;
        final WindowedStream<NewsFeed, Tuple, TimeWindow> sectionWindowedStream;
        final DataStream<Tuple6<Long, Long, List<Long>, String, String, Long>> result;
        final DataStream<Tuple5<Long, Long, List<Long>,  String, Long>> sectionResult;

        execEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        data= NewsFeedCustomParallelDataSource.createCustomPartitionData(parameterTool);
        eventStream = execEnv.addSource(new NewsFeedRichParallelSource(data, parameterTool));
        keyedDS = eventStream.keyBy("section","subSection");
        windowedStream = keyedDS.timeWindow(Time.seconds(20));

        sectionKeyedDS=keyedDS.keyBy("section");
        sectionWindowedStream=sectionKeyedDS.timeWindow((Time.seconds(10)));

        result = windowedStream.apply(new ApplyFunction4());
        sectionResult=sectionWindowedStream.apply(new ApplyFunction5());

        result.print();
        sectionResult.print();

        execEnv.execute("NewsFlink Ingestion Time Pipeline");
    }


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        TumblingIngestionimeUsingApplyExample window = new TumblingIngestionimeUsingApplyExample();
        window.executeJob(parameterTool);

    }



}
