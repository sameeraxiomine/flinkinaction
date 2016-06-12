package com.manning.fia.transformations.media;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

@SuppressWarnings("serial")
public class MapTokenizeNewsFeed implements MapFunction<NewsFeed, Tuple5<String, String, String, Long,Long>> {
    @Override
    public Tuple5<String, String, String, Long,Long> map(NewsFeed newsFeed) throws Exception {

        final long startTime = newsFeed.getStartTimeStamp();
        final long endTime = newsFeed.getEndTimeStamp();
        final long timeSpent = endTime - startTime;

        final Tuple5<String, String, String, Long,Long> tuple5 = new Tuple5<>(newsFeed.getSection(), newsFeed
                .getSubSection(), newsFeed.getTopic(), timeSpent,newsFeed.getPageId());
        return tuple5;
    }
}
