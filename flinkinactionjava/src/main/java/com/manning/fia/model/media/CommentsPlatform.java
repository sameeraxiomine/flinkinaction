package com.manning.fia.model.media;


import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple2;

@SuppressWarnings("serial")
public class CommentsPlatform implements Serializable{

    private long eventId;

    // pageId and pageTitle
    private Tuple2<Long, String> page;

    // the user commenting about the article
    private ApplicationUser applicationUser;

    // startTimestamp,endTimeStamp
    private Tuple2<Long, Long> timeStamp;

    private String comments;


    // sentimental Analysis score.
    private float sentimentAnalysisScore;


    public CommentsPlatform(long eventId, Tuple2<Long, String> page, ApplicationUser applicationUser, Tuple2<Long, Long> timeStamp, String comments) {
        this.eventId = eventId;
        this.page = page;
        this.applicationUser = applicationUser;
        this.timeStamp = timeStamp;
        this.comments = comments;
    }

    public Tuple2<Long, Long> getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Tuple2<Long, Long> timeStamp) {
        this.timeStamp = timeStamp;
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

    public ApplicationUser getApplicationUser() {
        return applicationUser;
    }

    public void setApplicationUser(ApplicationUser applicationUser) {
        this.applicationUser = applicationUser;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
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
        if (!(o instanceof CommentsPlatform)) return false;

        CommentsPlatform that = (CommentsPlatform) o;

        if (eventId != that.eventId) return false;
        if (!page.equals(that.page)) return false;
        if (!applicationUser.equals(that.applicationUser)) return false;
        if (!timeStamp.equals(that.timeStamp)) return false;
        return comments.equals(that.comments);

    }

    @Override
    public int hashCode() {
        int result = (int) (eventId ^ (eventId >>> 32));
        result = 31 * result + page.hashCode();
        result = 31 * result + applicationUser.hashCode();
        result = 31 * result + timeStamp.hashCode();
        result = 31 * result + comments.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CommentsPlatform{" +
                "eventId=" + eventId +
                ", page=" + page +
                ", applicationUser=" + applicationUser +
                ", timeStamp=" + timeStamp +
                ", comments='" + comments + '\'' +
                ", sentimentAnalysisScore=" + sentimentAnalysisScore +
                '}';
    }
}
