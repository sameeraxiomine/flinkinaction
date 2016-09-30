package com.manning.fia.c10;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.flink.api.common.functions.MapFunction;

public class NewsFeedSlidingMapper implements MapFunction<String, NewsFeed>{
  @Override
  public NewsFeed map(String value)
    throws Exception {
    NewsFeed newsFeed = NewsFeedParser.mapRowForNewsFeedWithSlide(value);
    return newsFeed;
  }
}
