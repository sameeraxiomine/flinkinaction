package com.manning.fia.transformations.media;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.utils.DateUtils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.joda.time.format.DateTimeFormat;

@SuppressWarnings("serial")
public class NewsFeedMapper8 implements MapFunction<String, Tuple3<String,Long, Long>> {
    @Override
    public Tuple3<String,Long, Long> map(String value)
            throws Exception {
        final NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        final Tuple3<String,Long, Long> tuple4 = new Tuple3<>(
                newsFeed.getUser().getUuid(),
                newsFeed.getPageId(),
                DateTimeFormat.forPattern("yyyyMMddHHmmss")
                        .parseDateTime(newsFeed.getStartTimeStamp()).getMillis());
        return tuple4;
    }
}

