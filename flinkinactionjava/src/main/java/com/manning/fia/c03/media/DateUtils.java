package com.manning.fia.c03.media;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.hadoop.shaded.com.google.common.base.Throwables;

import com.manning.fia.model.media.NewsFeed;

public class DateUtils implements Serializable{
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    public long getTimeSpentOnPage(NewsFeed event){
        long timeSpent = 0;
        try{
            Date startTs = sdf.parse(event.getStartTimeStamp());
            Date endTs = sdf.parse(event.getEndTimeStamp());
            timeSpent = endTs.getTime()-startTs.getTime();
        }catch(Exception ex){
            Throwables.propagate(ex);
        }
        return timeSpent;
    }
}
