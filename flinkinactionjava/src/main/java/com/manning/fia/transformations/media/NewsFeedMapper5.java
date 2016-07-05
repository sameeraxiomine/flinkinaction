package com.manning.fia.transformations.media;

import com.manning.fia.c03.media.DateUtils;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.joda.time.format.DateTimeFormat;

@SuppressWarnings("serial")
public class NewsFeedMapper5 implements
        MapFunction<String, Tuple7<Long, String, String, String, String, Long, Long>> {
    private DateUtils dateUtils = new DateUtils();

    @Override
    public Tuple7<Long, String, String, String, String, Long, Long> map(String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        final Tuple7<Long, String, String, String, String, Long, Long> tuple7 = new Tuple7<>(newsFeed.getEventId(),
                newsFeed.getSection(),
                newsFeed.getSubSection(),
                newsFeed.getTopic(),
                newsFeed.getDeviceType(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getStartTimeStamp())
                        .getMillis(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getEndTimeStamp()).getMillis()
        );
        return tuple7;

    }
}
