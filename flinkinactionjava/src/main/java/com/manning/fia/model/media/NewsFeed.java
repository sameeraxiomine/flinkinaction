package com.manning.fia.model.media;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;


public class NewsFeed {

    private long eventId;

    // pageId and pageTitle
    private Tuple2<Long, String> page;

    // section,subsection,topic,keywords
    private Tuple4<String, String, String, String[]> pageInfo;

    // startTimestamp,endTimeStamp
    private Tuple2<Long, Long> timeStamp;

    //mobile,web,nativeapp
    private String type;

    // details about the user who has read the page.
    private ApplicationUser user;

    // sentimental Analysis score.
    private float sentimentAnalysisScore;

    public NewsFeed() {
    }

    public NewsFeed(long eventId, Tuple2<Long, String> page, Tuple4<String, String, String, String[]> pageInfo, Tuple2<Long, Long> timeStamp, String type, ApplicationUser user) {
        this.eventId = eventId;
        this.page = page;
        this.pageInfo = pageInfo;
        this.timeStamp = timeStamp;
        this.type = type;

        this.user = user;

    }

    public long getEventId() {
        return eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public Tuple2<Long, String> getPage() {
        return page;
    }

    public void setPage(Tuple2<Long, String> page) {
        this.page = page;
    }

    public Tuple4<String, String, String, String[]> getPageInfo() {
        return pageInfo;
    }

    public void setPageInfo(Tuple4<String, String, String, String[]> pageInfo) {
        this.pageInfo = pageInfo;
    }

    public Tuple2<Long, Long> getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Tuple2<Long, Long> timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    public ApplicationUser getUser() {
        return user;
    }

    public void setUser(ApplicationUser user) {
        this.user = user;
    }

    public float getSentimentAnalysisScore() {
        return sentimentAnalysisScore;
    }

    public void setSentimentAnalysisScore(float sentimentAnalysisScore) {
        this.sentimentAnalysisScore = sentimentAnalysisScore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NewsFeed)) return false;

        NewsFeed newsFeed = (NewsFeed) o;

        if (eventId != newsFeed.eventId) return false;
        if (page != null ? !page.equals(newsFeed.page) : newsFeed.page != null) return false;
        if (pageInfo != null ? !pageInfo.equals(newsFeed.pageInfo) : newsFeed.pageInfo != null) return false;
        if (timeStamp != null ? !timeStamp.equals(newsFeed.timeStamp) : newsFeed.timeStamp != null) return false;
        if (type != null ? !type.equals(newsFeed.type) : newsFeed.type != null) return false;
        return user != null ? user.equals(newsFeed.user) : newsFeed.user == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (eventId ^ (eventId >>> 32));
        result = 31 * result + (page != null ? page.hashCode() : 0);
        result = 31 * result + (pageInfo != null ? pageInfo.hashCode() : 0);
        result = 31 * result + (timeStamp != null ? timeStamp.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        return result;
    }
}
