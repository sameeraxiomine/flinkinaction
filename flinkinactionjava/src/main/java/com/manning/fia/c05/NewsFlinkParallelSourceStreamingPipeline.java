package com.manning.fia.c05;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.utils.NewsFlinkRichParallelSource;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

/**
 * Created by hari on 6/26/16.
 */
public class NewsFlinkParallelSourceStreamingPipeline {

    private static String EMPTY_STRING = "";
    private Map<Integer, List<NewsFeed>> data;

    public void executeJob(ParameterTool parameterTool) throws Exception {
        data=new TreeMap<>();

        final String fileName = parameterTool.get("fileName");
        final int parallelism=parameterTool.getInt("parallelism",5);
        final int threadSleepInterval=parameterTool.getInt("threadSleepInterval",0);

        final List<String> newsFeeds = NewsFeedParser.parseData(fileName);
        List<NewsFeed> newsFeedList = new ArrayList<>(10);
        for (String newsFeed : newsFeeds) {
            NewsFeed newsFeed1 = NewsFeedParser.mapRow(newsFeed);
            newsFeedList.add(newsFeed1);
        }

        data.put(1, newsFeedList);

        for (NewsFeed newsFeed1 : newsFeedList) {
            for (int j = 2; j <= 10; j++) {
                List<NewsFeed> newsFeedList1 = new ArrayList<>(10);
                for (int i = 1; i <= 10; i++) {
                    NewsFeed newsFeed2 = SerializationUtils.clone(newsFeed1);
                    newsFeed2.setEventId(newsFeed1.getEventId() + i);
                    newsFeed2.setStartTimeStamp((Long.valueOf(newsFeed1.getStartTimeStamp()) + i) + EMPTY_STRING);
                    newsFeed2.setEndTimeStamp((Long.valueOf(newsFeed1.getEndTimeStamp()) + i) + EMPTY_STRING);
                    newsFeedList1.add(newsFeed2);
                }
                data.put(j, newsFeedList1);
            }
        }



        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(parallelism);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<NewsFeed> eventStream = execEnv.addSource(new NewsFlinkRichParallelSource(data, parameterTool));

        eventStream.print();

        execEnv.execute("NewsFlink Event Time Pipeline");
    }


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        NewsFlinkParallelSourceStreamingPipeline window = new NewsFlinkParallelSourceStreamingPipeline();
        window.executeJob(parameterTool);

    }



}
