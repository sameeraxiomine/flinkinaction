package com.manning.fia.utils.custom;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.format.DateTimeFormat;

import java.util.List;
import java.util.Map;

/**
 * This class will poll a folder every 5 seconds
 */

public class NewsFeedCustomDataSourceEmittingWM implements SourceFunction<NewsFeed> {
    private ParameterTool parameterTool;
    public NewsFeedCustomDataSourceEmittingWM(ParameterTool parameterTool) {
        this.parameterTool = parameterTool;
    }

    @Override
    public void run(SourceContext<NewsFeed> ctx) throws Exception {
        final String fileName = parameterTool.get("fileName");
        final List<String> newsFeeds = NewsFeedParser.parseData(fileName);
        int i = 1;
        NewsFeed newsFeed = null;
        long startTs = 0;
        for (String newsFeedString : newsFeeds) {
            newsFeed = NewsFeedParser.mapRow(newsFeedString);
            startTs = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .parseDateTime(newsFeed.getStartTimeStamp())
                    .getMillis();
            ctx.collectWithTimestamp(newsFeed, startTs);
            if (i % 5 == 0) {
                ctx.emitWatermark(new Watermark(startTs));
            }
            i++;
        }
        //for the last element
        if (newsFeed != null) {
            ctx.emitWatermark(new Watermark(startTs));
        }
    }
    
    @Override
    public void cancel() {
    }
}
