package com.manning.fia.utils.custom;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.format.DateTimeFormat;

import java.util.List;
import java.util.Map;

/**
 * This class will poll a folder every 5 seconds
 */

public class NewsFeedRichParallelSource extends RichParallelSourceFunction<NewsFeed> {

    private Map<Integer, List<NewsFeed>> data;
    private Map<Integer, Long> delaysBetweenEvents;
    private long threadSleepInterval;
    private int index = -1;
    private int watermarkEventNEvents = 5;


    public NewsFeedRichParallelSource(Map<Integer, List<NewsFeed>> data, ParameterTool parameterTool) {
        this.data = data;
        this.threadSleepInterval = parameterTool.getInt("threadSleepInterval", 0);
    }

    @Override
    public void open(Configuration configuration) {
        int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
        this.index = getRuntimeContext().getIndexOfThisSubtask();

    }

    @Override
    public void cancel() {

    }

    @Override
    public void run(SourceContext<NewsFeed> ctx) throws Exception {

        NewsFeed newsFeed = null;
        List<NewsFeed> myData = data.get(this.index);
        for (int i = 0; i < myData.size(); i++) {
            newsFeed = myData.get(i);
            long startTs = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .parseDateTime(newsFeed.getStartTimeStamp())
                    .getMillis();
            ctx.collectWithTimestamp(newsFeed, startTs);
            Thread.currentThread().sleep(this.threadSleepInterval);

            if (i % this.watermarkEventNEvents == 0) {
                ctx.emitWatermark(new Watermark(startTs));
            }
        }
        //Emit one more watermark;
        if (newsFeed != null) {
            long startTs = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .parseDateTime(newsFeed.getStartTimeStamp())
                    .getMillis();
            ctx.emitWatermark(new Watermark(startTs));
        }

    }

}
