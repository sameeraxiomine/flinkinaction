package com.manning.fia.transformations.media;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by hari on 6/16/16.
 */
public class DomainObjectBasedNewsFeedMapper implements
        MapFunction<String,NewsFeed> {
    @Override
    public NewsFeed map(String value) throws Exception {
        NewsFeed newsFeed=NewsFeedParser.mapRow(value);
        return newsFeed;
    }
}
