package com.manning.fia.model.media;

@SuppressWarnings("serial")
public class WaterMarkedNewsFeed extends NewsFeed {
   public WaterMarkedNewsFeed(long eventId, String startTimeStamp, long pageId, String referrer, String section, String subSection, String topic, String[] keywords, String endTimeStamp, String deviceType, ApplicationUser user) {
      super(eventId, startTimeStamp,  pageId,  referrer,  section,  subSection,  topic,  keywords,  endTimeStamp,  deviceType,  user);
  }
}
