package com.manning.fia.transformations.media;

import com.manning.fia.model.media.*;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


@SuppressWarnings("serial")
public class NewsFeedParser {


    public static List<String> parseData() throws Exception {
        return parseData("/media/pipe/newsfeed");
    }

    public static List<String> parseData(String file) throws Exception {
        final Scanner scanner = new Scanner(NewsFeedParser.class.getResourceAsStream(file));
        List<String> newsFeeds = new ArrayList<>();
        while (scanner.hasNext()) {
            String value = scanner.nextLine();
            newsFeeds.add(value);
        }
        return newsFeeds;
    }

    public static NewsFeed mapRow(String value) {
        final NewsFeed newsFeed;
        final String[] tokens = StringUtils.splitPreserveAllTokens(value, "|");


        final long eventId = Long.valueOf(tokens[0]);
        final long pageId = Long.valueOf(tokens[1]);

        final String referrer = tokens[2];

        final String section = tokens[3];
        final String subSection = tokens[4];
        final String topic = tokens[5];
        final String keywordString = tokens[6];
        final String[] keywords = keywordString.split(":");

        final String startTimeStamp = tokens[7];
        final String endTimeStamp = tokens[8];
        final String deviceType = tokens[9];

        final String uuid = tokens[10];
        final String subscriberId = tokens[11];
        final String ipAddress = tokens[12];
        final ApplicationUser applicationUser = new ApplicationUser(uuid, subscriberId, ipAddress);
        newsFeed = new NewsFeed(eventId, startTimeStamp, pageId, referrer, section, subSection, topic, keywords,
                endTimeStamp, deviceType, applicationUser);


        return newsFeed;
    }



}
