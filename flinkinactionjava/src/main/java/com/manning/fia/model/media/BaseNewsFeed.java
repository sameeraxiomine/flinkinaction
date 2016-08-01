package com.manning.fia.model.media;

import java.io.Serializable;

/**
 * Created by hari on 7/31/16.
 */
public class BaseNewsFeed implements Serializable{

    private long eventId;

    private String startTimeStamp;


    public BaseNewsFeed(long eventId, String startTimeStamp) {

        this.eventId = eventId;
        this.startTimeStamp = startTimeStamp;
    }

    public BaseNewsFeed() {
    }

    public long getEventId() {
        return eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public String getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(String startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseNewsFeed)) return false;

        BaseNewsFeed that = (BaseNewsFeed) o;

        if (eventId != that.eventId) return false;
        return startTimeStamp != null ? startTimeStamp.equals(that.startTimeStamp) : that.startTimeStamp == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (eventId ^ (eventId >>> 32));
        result = 31 * result + (startTimeStamp != null ? startTimeStamp.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NewsFeed{" +
                "eventId=" + eventId +
                ", startTimeStamp='" + startTimeStamp + '\'' +
                '}';
    }
}
