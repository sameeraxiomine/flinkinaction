package com.manning.fia.transformations.media;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

import com.manning.fia.utils.DateUtils;
import com.manning.fia.model.media.NewsFeed;

@SuppressWarnings("serial")
public class NewsFeedMapper2 implements MapFunction<String, Tuple5<String, String,Long, Long,Long>> {
    private DateUtils dateUtils = new DateUtils();
    @Override
    public Tuple5<String, String,Long, Long,Long> map(String value)
            throws Exception {
        NewsFeed newsFeed=NewsFeedParser.mapRow(value);
        long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);
        Tuple5<String, String,Long, Long,Long> tuple5 = new Tuple5<>(newsFeed.getSection(), 
                                                                     newsFeed.getSubSection(), 
                                                                     Long.valueOf(newsFeed.getStartTimeStamp()),
                                                                     Long.valueOf(newsFeed.getEndTimeStamp()),
                                                                     timeSpent);
        return tuple5;
    }
}

