package com.manning.fia.c03.media;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.hadoop.shaded.com.google.common.base.Throwables;

import com.manning.fia.model.media.NewsFeed;
import org.joda.time.format.DateTimeFormat;

public class DateUtils implements Serializable {

    public long getTimeSpentOnPage(NewsFeed event) {
        long timeSpent = 0;
        try {
            long startTs = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                                          .parseDateTime(event.getStartTimeStamp())
                                          .getMillis();

            long endTs = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                                        .parseDateTime(event.getEndTimeStamp())
                                        .getMillis();

            timeSpent = endTs - startTs;
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        return timeSpent;
    }
}
