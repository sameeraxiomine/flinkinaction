package com.manning.fia.transformations.media;

import org.apache.flink.api.common.functions.MapFunction;

import com.manning.fia.model.media.NewsFeed;

@SuppressWarnings("serial")
public class ExtractIPAddressMapper implements
        MapFunction<String, String> {
    @Override
    public String map(String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        return newsFeed.getUser().getIpAddress();       

    }
}
