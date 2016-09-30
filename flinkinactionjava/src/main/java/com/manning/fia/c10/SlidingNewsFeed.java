package com.manning.fia.c10;

import com.manning.fia.model.media.ApplicationUser;
import com.manning.fia.model.media.NewsFeed;

public class SlidingNewsFeed extends NewsFeed {
  private long slide;

  public SlidingNewsFeed() {
  }

  public SlidingNewsFeed(long eventId, String startTimeStamp, long pageId, String referrer, String section, String subSection,
                         String topic, String[] keywords, String endTimeStamp, String deviceType,
                         ApplicationUser user, long slide) {
    super(eventId, startTimeStamp, pageId, referrer, section, subSection, topic, keywords,
      endTimeStamp, deviceType, user);
    this.slide = slide;
  }

  public long getSlide() {
    return slide;
  }

  public void setSlide(final long slide) {
    this.slide = slide;
  }
}
