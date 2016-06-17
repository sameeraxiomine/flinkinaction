package com.manning.fia.transformations.media;

import com.manning.fia.model.media.ApplicationUser;
import com.manning.fia.model.media.NewsFeed;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


@SuppressWarnings("serial")
public class NewsFeedParser {


    public static List<String> parseData() throws Exception {
        final Scanner scanner = new Scanner(ClassLoader.class.getResourceAsStream("/media/pipe/newsfeed"));
        List<String> newsFeeds = new ArrayList<>(0);
        while (scanner.hasNext()) {
            String value = scanner.nextLine();
            newsFeeds.add(value);
        }
        return newsFeeds;
    }


    public static NewsFeed mapRow(String value) {

        final String[] tokens = value.toLowerCase().split("\\|");

        final long eventId = Long.valueOf(tokens[0]);
        final long pageId = Long.valueOf(tokens[1]);

        final String referrer = tokens[2];

        final String section = tokens[3];
        final String subSection = tokens[4];
        final String topic = tokens[5];
        final String keywordString = tokens[6];
        final String[] keywords = keywordString.split(":");

        final long startTimeStamp = Long.valueOf(tokens[7]);
        final long endTimeStamp = Long.valueOf(tokens[8]);
        final String type = tokens[9];

        final String uuid = tokens[10];
        final String subscriberId = tokens[11];
        final String ipAddress = tokens[12];

        final ApplicationUser applicationUser = new ApplicationUser(uuid, subscriberId, ipAddress);

        final NewsFeed newsFeed = new NewsFeed(eventId, pageId, referrer, section, subSection, topic, keywords,
                startTimeStamp, endTimeStamp, type, applicationUser);
        return newsFeed;
    }

}
