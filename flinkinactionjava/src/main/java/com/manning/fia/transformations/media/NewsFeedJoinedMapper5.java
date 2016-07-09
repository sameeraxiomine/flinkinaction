package com.manning.fia.transformations.media;

import com.manning.fia.utils.DateUtils;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.joda.time.format.DateTimeFormat;

@SuppressWarnings("serial")
public class NewsFeedJoinedMapper5 implements
        MapFunction<String, Tuple6<Long, String, String, String, Long, Long>> {
    private DateUtils dateUtils = new DateUtils();

    @Override
    public Tuple6<Long, String, String, String, Long, Long> map(String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        final Tuple6<Long, String, String, String, Long, Long> tuple6 = new Tuple6<>(
                newsFeed.getEventId(),
                newsFeed.getSection(),
                newsFeed.getSubSection(),
                newsFeed.getDeviceType(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getStartTimeStamp())
                        .getMillis(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getEndTimeStamp()).getMillis()
        );
        return tuple6;

    }
}
