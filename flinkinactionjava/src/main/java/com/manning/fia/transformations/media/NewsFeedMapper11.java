package com.manning.fia.transformations.media;

import com.manning.fia.model.media.BaseNewsFeed;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;

@SuppressWarnings("serial")
public class NewsFeedMapper11 implements MapFunction<String, NewsFeed> {
    @Override
    public NewsFeed map(String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRow2(value);
        return newsFeed;
    }
}

