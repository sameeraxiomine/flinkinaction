package com.manning.fia.transformations.media;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.utils.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.joda.time.format.DateTimeFormat;

@SuppressWarnings("serial")
public class NewsFeedMapper8 implements MapFunction<String, Tuple4<Long, Long, Long, String>> {
    private DateUtils dateUtils = new DateUtils();

    @Override
    public Tuple4<Long, Long, Long, String> map(String value)
            throws Exception {
        final NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        final long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);
        final Tuple4<Long, Long, Long, String> tuple4 = new Tuple4<>(newsFeed.getPageId(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getStartTimeStamp()).getMillis(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getEndTimeStamp()).getMillis(),
                newsFeed.getUser().getUuid());
        return tuple4;
    }
}

