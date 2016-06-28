package com.manning.fia.transformations.media;

import com.manning.fia.c03.media.DateUtils;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

@SuppressWarnings("serial")
public class NewsFeedMapper3 implements MapFunction<String, Tuple6<Long,String, String,Long, Long,Long>> {
    private DateUtils dateUtils = new DateUtils();
    @Override
    public Tuple6<Long,String, String,Long, Long,Long> map(String value)
            throws Exception {
        NewsFeed newsFeed=NewsFeedParser.mapRow(value);

        long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);

        Tuple6<Long,String, String,Long, Long,Long> tuple6 = new Tuple6<>(
                                                                     newsFeed.getEventId(),
                                                                     newsFeed.getSection(),
                                                                     newsFeed.getSubSection(),
                                                                     Long.valueOf(newsFeed.getStartTimeStamp()),
                                                                     Long.valueOf(newsFeed.getEndTimeStamp()),
                                                                     timeSpent);
        return tuple6;
    }
}

