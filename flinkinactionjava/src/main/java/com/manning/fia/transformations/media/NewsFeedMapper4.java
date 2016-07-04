package com.manning.fia.transformations.media;

import com.manning.fia.model.media.NewsFeed;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

@SuppressWarnings("serial")
public class NewsFeedMapper4 implements
        MapFunction<String, Tuple4<Long, String, String, String>> {
    @Override
    public Tuple4<Long, String, String, String> map(String value)
            throws Exception {
        Thread.currentThread().sleep(8000);
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        return new Tuple4<>(newsFeed.getEventId(), newsFeed.getTopic(),
                newsFeed.getStartTimeStamp(), newsFeed.getEndTimeStamp());

    }
}
