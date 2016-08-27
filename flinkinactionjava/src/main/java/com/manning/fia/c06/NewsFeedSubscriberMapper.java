package com.manning.fia.c06;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public class NewsFeedSubscriberMapper implements
  MapFunction<String, Tuple6<String, Long, String, String, String, String>> {

  @Override
  public Tuple6<String, Long, String, String, String, String> map(String value) throws Exception {

    NewsFeed newsFeed = NewsFeedParser.mapRow(value);

    return new Tuple6<>(newsFeed.getUser().getSubscriberId(),
      newsFeed.getEventId(),
      newsFeed.getSection(),
      newsFeed.getSubSection(),
      newsFeed.getStartTimeStamp(),
      newsFeed.getEndTimeStamp());
  }
}
