package com.manning.fia.transformations.media;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.manning.fia.model.media.ApplicationUser;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;


@SuppressWarnings("serial")
public class NewsFeedParser {


    public static List<NewsFeed> parse(boolean isJSON) throws Exception {
        if (isJSON) {
            return parseJSON();
        }
        return parseCSV();
    }

    private static List<NewsFeed> parseCSV() throws Exception {
        final Scanner scanner = new Scanner(ClassLoader.class.getResourceAsStream("/media/csv/newsfeed.csv"));
        List<NewsFeed> newsFeeds = new ArrayList<>(0);
        while (scanner.hasNext()) {
            String value = scanner.nextLine();
            newsFeeds.add(mapCSVRow(value));
        }
        return newsFeeds;
    }


    public static NewsFeed mapCSVRow(String value) {

        final String[] tokens = value.toLowerCase().split(",");

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

    public static List<NewsFeed> parseJSON() throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        final NewsFeed[] newsFeeds = objectMapper.readValue(ClassLoader.class.getResourceAsStream
                ("/media/json/newsfeed.json"), NewsFeed[].class);
        return Arrays.asList(newsFeeds);
    }
}
