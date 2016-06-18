package com.manning.fia.transformations.media;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

import com.manning.fia.c03.media.DateUtils;
import com.manning.fia.model.media.NewsFeed;

@SuppressWarnings("serial")
public class NewsFeedMapper implements MapFunction<String, Tuple5<Long, String, String,String, Long>> {
    private DateUtils dateUtils = new DateUtils();
    @Override
    public Tuple5<Long, String, String, String, Long> map(String value)
            throws Exception {
        final NewsFeed newsFeed=NewsFeedParser.mapRow(value);
        final long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);
        final Tuple5<Long, String, String, String, Long> tuple5 = new Tuple5<>(newsFeed.getPageId(),
                                                                               newsFeed.getSection(), 
                                                                               newsFeed.getSubSection(), 
                                                                               newsFeed.getTopic(), 
                                                                               timeSpent);
        return tuple5;
    }
}

