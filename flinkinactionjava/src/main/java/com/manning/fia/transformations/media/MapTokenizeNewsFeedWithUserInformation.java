package com.manning.fia.transformations.media;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class MapTokenizeNewsFeedWithUserInformation implements MapFunction<String, Tuple4<String, String, String,Long>> {
    @Override
    public Tuple4<String, String, String,Long> map(
            String value) throws Exception {

//        final NewsFeed newsFeed = NewsFeedParser.map(value);
//
//        final long pageId = newsFeed.getPageId();
//
//        final String section = newsFeed.getSection()
//        final String subsection = newsFeed.getPageInfo().f1;
//        final String topic=newsFeed.getPageInfo().f2;
//
//        final long startTime = newsFeed.getTimeStamp().f0;
//        final long endTime = newsFeed.getTimeStamp().f1;
//        final long timeSpent=endTime-startTime;
//
//        final Tuple4<String, String, String,Long> tuple4 = new Tuple4<>(section, subsection, topic,timeSpent);
//        return tuple4;
    return null;
    }
}
