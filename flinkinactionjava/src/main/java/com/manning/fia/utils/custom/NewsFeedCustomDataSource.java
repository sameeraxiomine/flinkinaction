package com.manning.fia.utils.custom;

import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

/**
 * Created by hari on 7/17/16.
 */
public class NewsFeedCustomDataSource implements SourceFunction<String>{


    private ParameterTool parameterTool;

    public NewsFeedCustomDataSource(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        final String fileName=parameterTool.get("fileName","/media/pipe/newsfeed");
        final List<String> newsFeeds = NewsFeedParser.parseData(fileName);
        final int threadSleepInterval=parameterTool.getInt("threadSleepInterval",0);
        for (String newsFeed : newsFeeds) {
            sourceContext.collect(newsFeed);
            Thread.currentThread().sleep(threadSleepInterval);
        }
    }

    @Override
    public void cancel() {

    }

}
