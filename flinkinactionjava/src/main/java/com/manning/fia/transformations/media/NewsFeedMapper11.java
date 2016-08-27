package com.manning.fia.transformations.media;

import org.apache.flink.api.common.functions.MapFunction;

import com.manning.fia.model.media.NewsFeed;

@SuppressWarnings("serial")
public class NewsFeedMapper11 implements MapFunction<String, NewsFeed> {
    @Override
    public NewsFeed map(String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRowForNewsFeedWithWM(value);
        return newsFeed;
    }
}

