package com.manning.fia.transformations;

import com.manning.fia.model.media.ApplicationUser;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class NewsFeedParser {
    public static NewsFeed map(String value) throws Exception {

        final String[] tokens = value.toLowerCase().split(",");
        final long eventId = Long.valueOf(tokens[0]);
        final long pageId = Long.valueOf(tokens[1]);
        final String pageTitle = tokens[2];
        final String section = tokens[3];
        final String subSection = tokens[4];
        final String topic = tokens[5];
        final String keywordString = tokens[6];
        final String[] keywords = keywordString.split(":");
        final long startTime = Long.valueOf(tokens[7]);
        final long endTime = Long.valueOf(tokens[8]);
        final String type = tokens[9];
        final String uuid = tokens[10];
        final String subscriberId = tokens[11];
        final String ipAddress = tokens[12];

        final ApplicationUser applicationUser = new ApplicationUser(uuid, subscriberId, ipAddress);
        final Tuple2<Long, String> page = new Tuple2<>(pageId, pageTitle);
        final Tuple4<String, String, String, String[]> pageInfo = new Tuple4<>(section, subSection, topic, keywords);
        final Tuple2<Long, Long> timeStamp = new Tuple2<>(startTime, endTime);
        final NewsFeed newsFeed = new NewsFeed(eventId, page, pageInfo, timeStamp, type, applicationUser);
        return newsFeed;
    }
}
