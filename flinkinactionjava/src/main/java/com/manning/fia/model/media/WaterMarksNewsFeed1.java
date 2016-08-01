package com.manning.fia.model.media;

@SuppressWarnings("serial")
public class WaterMarksNewsFeed1 extends BaseNewsFeed {

    public boolean waterMark;

    public WaterMarksNewsFeed1(long eventId, String startTimeStamp, boolean waterMark) {
        super(eventId, startTimeStamp);
        this.waterMark = waterMark;
    }

    public WaterMarksNewsFeed1() {
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
        if (!(o instanceof WaterMarksNewsFeed1)) return false;
        if (!super.equals(o)) return false;

        WaterMarksNewsFeed1 that = (WaterMarksNewsFeed1) o;

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
        return "WaterMarksNewsFeed{" +
                "waterMark=" + waterMark +
                '}';
    }
}
