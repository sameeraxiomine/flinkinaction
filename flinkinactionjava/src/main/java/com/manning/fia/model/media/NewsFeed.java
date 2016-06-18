package com.manning.fia.model.media;

import java.util.Arrays;

public class NewsFeed {

    private long eventId;

    // pageId
    private long pageId;

    private String referrer;

    // section
    private String section;

    // subsection
    private String subSection;

    // topic
    private String topic;

    private String[] keywords;

    // startTimestamp,endTimeStamp
    private String startTimeStamp;

    private String endTimeStamp;

    // mobile,web,nativeapp
    private String deviceType;

    // details about the user who has read the page.
    private ApplicationUser user;

    public NewsFeed() {
    }

    public NewsFeed(long eventId, long pageId, String referrer, String section,
            String subSection, String topic, String[] keywords,
            String startTimeStamp, String endTimeStamp, String type,
            ApplicationUser user) {
        this.eventId = eventId;
        this.pageId = pageId;
        this.referrer = referrer;
        this.section = section;
        this.subSection = subSection;
        this.topic = topic;
        this.keywords = keywords;
        this.startTimeStamp = startTimeStamp;
        this.endTimeStamp = endTimeStamp;
        this.deviceType = type;
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

    public String getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(String startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public String getEndTimeStamp() {
        return endTimeStamp;
    }

    public void setEndTimeStamp(String endTimeStamp) {
        this.endTimeStamp = endTimeStamp;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public ApplicationUser getUser() {
        return user;
    }

    public void setUser(ApplicationUser user) {
        this.user = user;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (eventId ^ (eventId >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NewsFeed other = (NewsFeed) obj;
        if (eventId != other.eventId)
            return false;
        return true;
    }

  
    @Override
    public String toString() {
        return "NewsFeed{" + "eventId=" + eventId + ", pageId=" + pageId
                + ", referrer='" + referrer + '\'' + ", section='" + section
                + '\'' + ", subSection='" + subSection + '\'' + ", topic='"
                + topic + '\'' + ", keywords=" + Arrays.toString(keywords)
                + ", startTimeStamp=" + startTimeStamp + ", endTimeStamp="
                + endTimeStamp + ", type='" + deviceType + '\'' + ", user=" + user
                + '}';
    }
}
