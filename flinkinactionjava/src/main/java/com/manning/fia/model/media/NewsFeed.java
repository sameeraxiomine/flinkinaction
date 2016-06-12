package com.manning.fia.model.media;

import java.util.Arrays;


public class NewsFeed {

    private long eventId;

    // pageId
    private long pageId;

    private String referrer;

    //section
    private String section;

    //subsection
    private String subSection;

    //topic
    private String topic;

    private String[] keywords;

    // startTimestamp,endTimeStamp
    private long startTimeStamp;

    private long endTimeStamp;

    //mobile,web,nativeapp
    private String type;

    // details about the user who has read the page.
    private ApplicationUser user;

    public NewsFeed() {
    }

    public NewsFeed(long eventId, long pageId, String referrer, String section, String subSection, String topic, String[] keywords, long startTimeStamp, long endTimeStamp, String type, ApplicationUser user) {
        this.eventId = eventId;
        this.pageId = pageId;
        this.referrer = referrer;
        this.section = section;
        this.subSection = subSection;
        this.topic = topic;
        this.keywords = keywords;
        this.startTimeStamp = startTimeStamp;
        this.endTimeStamp = endTimeStamp;
        this.type = type;
        this.user = user;
    }

    public long getEventId() {
        return eventId;
    }

    public void setEventId(long eventId) {
        this.eventId = eventId;
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public String getReferrer() {
        return referrer;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public String getSection() {
        return section;
    }

    public void setSection(String section) {
        this.section = section;
    }

    public String getSubSection() {
        return subSection;
    }

    public void setSubSection(String subSection) {
        this.subSection = subSection;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String[] getKeywords() {
        return keywords;
    }

    public void setKeywords(String[] keywords) {
        this.keywords = keywords;
    }

    public long getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(long startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public long getEndTimeStamp() {
        return endTimeStamp;
    }

    public void setEndTimeStamp(long endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
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

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (!(o instanceof NewsFeed)) return false;

        NewsFeed that = (NewsFeed) o;

        if (eventId != that.eventId) return false;
        if (pageId != that.pageId) return false;
        if (startTimeStamp != that.startTimeStamp) return false;
        if (endTimeStamp != that.endTimeStamp) return false;
        if (referrer != null ? !referrer.equals(that.referrer) : that.referrer != null) return false;
        if (section != null ? !section.equals(that.section) : that.section != null) return false;
        if (subSection != null ? !subSection.equals(that.subSection) : that.subSection != null) return false;
        if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(keywords, that.keywords)) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return user != null ? user.equals(that.user) : that.user == null;

    }

    @Override
    public int hashCode() {
        int result = (int) (eventId ^ (eventId >>> 32));
        result = 31 * result + (int) (pageId ^ (pageId >>> 32));
        result = 31 * result + (referrer != null ? referrer.hashCode() : 0);
        result = 31 * result + (section != null ? section.hashCode() : 0);
        result = 31 * result + (subSection != null ? subSection.hashCode() : 0);
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(keywords);
        result = 31 * result + (int) (startTimeStamp ^ (startTimeStamp >>> 32));
        result = 31 * result + (int) (endTimeStamp ^ (endTimeStamp >>> 32));
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NewsFeed{" +
                "eventId=" + eventId +
                ", pageId=" + pageId +
                ", referrer='" + referrer + '\'' +
                ", section='" + section + '\'' +
                ", subSection='" + subSection + '\'' +
                ", topic='" + topic + '\'' +
                ", keywords=" + Arrays.toString(keywords) +
                ", startTimeStamp=" + startTimeStamp +
                ", endTimeStamp=" + endTimeStamp +
                ", type='" + type + '\'' +
                ", user=" + user +
                '}';
    }
}
