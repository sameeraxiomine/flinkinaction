package com.manning.fia.utils;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.hadoop.shaded.com.google.common.base.Throwables;
import org.joda.time.format.DateTimeFormat;

import java.io.Serializable;

/**
 * util class to calculate time spent.
 *
 */
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
