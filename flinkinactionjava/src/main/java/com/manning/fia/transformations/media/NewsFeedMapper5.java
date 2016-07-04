package com.manning.fia.transformations.media;

import com.manning.fia.c03.media.DateUtils;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class NewsFeedMapper5 implements
        MapFunction<String, Long> {
    private DateUtils dateUtils = new DateUtils();
    @Override
    public Long map(String value)
            throws Exception {
        Thread.currentThread().sleep(8000);
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        final long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);
        return timeSpent;

    }
}
