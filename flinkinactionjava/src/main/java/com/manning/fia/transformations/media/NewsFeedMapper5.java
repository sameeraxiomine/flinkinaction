package com.manning.fia.transformations.media;

import com.manning.fia.utils.DateUtils;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.joda.time.format.DateTimeFormat;

@SuppressWarnings("serial")
public class NewsFeedMapper5 implements
        MapFunction<String, Tuple7<Long, Long, String, String, String, Long, Long>> {
    private DateUtils dateUtils = new DateUtils();

    @Override
    public Tuple7<Long, Long, String, String, String, Long, Long> map(String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        final Tuple7<Long, Long, String, String, String, Long, Long> tuple7 = new Tuple7<>(
                newsFeed.getEventId(),
                newsFeed.getPageId(),
                newsFeed.getReferrer(),
                newsFeed.getSection(),
                newsFeed.getSubSection(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getStartTimeStamp())
                        .getMillis(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getEndTimeStamp()).getMillis()
        );
        return tuple7;

    }
}
