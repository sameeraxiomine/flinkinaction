package com.manning.fia.transformations.media;

import com.manning.fia.model.media.BaseNewsFeed;
import org.apache.flink.api.common.functions.MapFunction;

@SuppressWarnings("serial")
public class NewsFeedMapper10 implements MapFunction<String, BaseNewsFeed> {
    @Override
    public BaseNewsFeed map(String value)
            throws Exception {
        BaseNewsFeed newsFeed = NewsFeedParser.mapRow1(value);
        return newsFeed;
    }
}

