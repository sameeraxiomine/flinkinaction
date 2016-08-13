package com.manning.fia.model.media;

@SuppressWarnings("serial")
public class WaterMarksNewsFeed extends NewsFeed {

    public boolean waterMark;

    public WaterMarksNewsFeed(boolean waterMark) {
        this.waterMark = waterMark;
    }

    public WaterMarksNewsFeed(long eventId, String startTimeStamp, long pageId, String referrer, String section,
                              String subSection, String topic, String[] keywords, String endTimeStamp, String type, ApplicationUser user, boolean waterMark) {
        super(eventId, startTimeStamp, pageId, referrer, section, subSection, topic, keywords, endTimeStamp, type, user);
        this.waterMark = waterMark;
    }

    public boolean isWaterMark() {
        return waterMark;
    }

    public void setWaterMark(boolean waterMark) {
        this.waterMark = waterMark;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WaterMarksNewsFeed)) return false;
        if (!super.equals(o)) return false;

        WaterMarksNewsFeed that = (WaterMarksNewsFeed) o;

        return waterMark == that.waterMark;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (waterMark ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WaterMarksNewsFeed1{" +
                "waterMark=" + waterMark + super.toString() +
                '}';
    }
}
