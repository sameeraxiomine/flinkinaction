package com.manning.fia.transformations.media;

import com.manning.fia.utils.DateUtils;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;

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
