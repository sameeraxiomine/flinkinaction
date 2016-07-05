package com.manning.fia.transformations.media;

import com.manning.fia.c03.media.DateUtils;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.joda.time.format.DateTimeFormat;

@SuppressWarnings("serial")
public class NewsFeedMapper6 implements
        MapFunction<String, NewsFeed> {
    private DateUtils dateUtils = new DateUtils();

    @Override
    public NewsFeed map(String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);
       return newsFeed;

    }
}
