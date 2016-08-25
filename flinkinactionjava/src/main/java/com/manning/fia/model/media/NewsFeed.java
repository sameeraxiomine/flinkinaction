package com.manning.fia.model.media;

import java.util.Arrays;

@SuppressWarnings("serial")
public class NewsFeed extends BaseNewsFeed {


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


    private String endTimeStamp;

    // mobile,web,nativeapp
    private String deviceType;

    // details about the user who has read the page.
    private ApplicationUser user;

    public NewsFeed() {
    }

    public NewsFeed(long eventId, String startTimeStamp, long pageId, String referrer, String section, String subSection, String topic, String[] keywords, String endTimeStamp, String deviceType, ApplicationUser user) {
        super(eventId, startTimeStamp);
        this.pageId = pageId;
        this.referrer = referrer;
        this.section = section;
        this.subSection = subSection;
        this.topic = topic;
        this.keywords = keywords;
        this.endTimeStamp = endTimeStamp;
        this.deviceType = deviceType;
        this.user = user;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NewsFeed)) return false;
        if (!super.equals(o)) return false;

        NewsFeed NewsFeed = (NewsFeed) o;

        if (pageId != NewsFeed.pageId) return false;
        if (referrer != null ? !referrer.equals(NewsFeed.referrer) : NewsFeed.referrer != null) return false;
        if (section != null ? !section.equals(NewsFeed.section) : NewsFeed.section != null) return false;
        if (subSection != null ? !subSection.equals(NewsFeed.subSection) : NewsFeed.subSection != null) return false;
        if (topic != null ? !topic.equals(NewsFeed.topic) : NewsFeed.topic != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(keywords, NewsFeed.keywords)) return false;
        if (endTimeStamp != null ? !endTimeStamp.equals(NewsFeed.endTimeStamp) : NewsFeed.endTimeStamp != null)
            return false;
        if (deviceType != null ? !deviceType.equals(NewsFeed.deviceType) : NewsFeed.deviceType != null) return false;
        return user != null ? user.equals(NewsFeed.user) : NewsFeed.user == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (pageId ^ (pageId >>> 32));
        result = 31 * result + (referrer != null ? referrer.hashCode() : 0);
        result = 31 * result + (section != null ? section.hashCode() : 0);
        result = 31 * result + (subSection != null ? subSection.hashCode() : 0);
        result = 31 * result + (topic != null ? topic.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(keywords);
        result = 31 * result + (endTimeStamp != null ? endTimeStamp.hashCode() : 0);
        result = 31 * result + (deviceType != null ? deviceType.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "NewsFeed{" +
                "pageId=" + pageId +
                ", referrer='" + referrer + '\'' +
                ", section='" + section + '\'' +
                ", subSection='" + subSection + '\'' +
                ", topic='" + topic + '\'' +
                ", keywords=" + Arrays.toString(keywords) +
                ", endTimeStamp='" + endTimeStamp + '\'' +
                ", deviceType='" + deviceType + '\'' +
                ", user=" + user +
                super.toString() +
                '}';
    }
}
