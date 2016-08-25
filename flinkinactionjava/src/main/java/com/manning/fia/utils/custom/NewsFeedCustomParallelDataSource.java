package com.manning.fia.utils.custom;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.*;

/**
 * Created by hari on 8/22/16.
 */
public class NewsFeedCustomParallelDataSource {


    public static Map<Integer, List<NewsFeed>> createCustomPartitionData(ParameterTool parameterTool) throws Exception {
        final Map<Integer, List<NewsFeed>> data;
        final List<String> newsFeeds;
        final List<NewsFeed> baseNewsFeeds;
        final String fileName = parameterTool.get("fileName");


        data = new TreeMap<>();
        newsFeeds = NewsFeedParser.parseData(fileName);
        baseNewsFeeds = new ArrayList<>(10);

        Map<String, List<NewsFeed>> internalNewsFeedMap = new HashMap<>();

        //Create the baseNewFeeds and partition by ip address.
        for (String newsFeedString : newsFeeds) {
            NewsFeed newsFeed = NewsFeedParser.mapRow(newsFeedString);
            if (internalNewsFeedMap.containsKey(newsFeed.getUser().getIpAddress())) {
                internalNewsFeedMap.get(newsFeed.getUser().getIpAddress()).add(newsFeed);
            } else {
                List<NewsFeed> list = new ArrayList<>(1);
                list.add(newsFeed);
                internalNewsFeedMap.put(newsFeed.getUser().getIpAddress(), list);
            }
            baseNewsFeeds.add(newsFeed);
        }


        List<NewsFeed> loopNewsFeeds = baseNewsFeeds;
        List<NewsFeed> otherNewsFeeds;

        int startSecondsToIncrease;
        int endSecondsToIncrease = 100;

        for (int j = 1; j <= 4; j++) {
            // reason for this is because we are using YYYYMMddHHmmss format.
            if (j % 3 == 0) {
                startSecondsToIncrease = 60;
            } else {
                startSecondsToIncrease = 20;
            }
            NewsFeed clonedNewsFeed;
            otherNewsFeeds = new ArrayList<>(10);
            for (NewsFeed newsFeed : loopNewsFeeds) {
                clonedNewsFeed = SerializationUtils.clone(newsFeed);
                clonedNewsFeed.setEventId(newsFeed.getEventId() + 10);

                Long newStartTimeStamp = Long.valueOf(newsFeed.getStartTimeStamp()) + startSecondsToIncrease;
                Long newEndTimeStamp = Long.valueOf(newsFeed.getEndTimeStamp()) + endSecondsToIncrease;

                clonedNewsFeed.setStartTimeStamp(newStartTimeStamp + StringUtils.EMPTY);
                clonedNewsFeed.setEndTimeStamp(newEndTimeStamp + StringUtils.EMPTY);

                otherNewsFeeds.add(clonedNewsFeed);
                // since we are using the same ip address we don't need to check, just add.
                internalNewsFeedMap.get(newsFeed.getUser().getIpAddress()).add(clonedNewsFeed);
            }
            loopNewsFeeds = otherNewsFeeds;
        }
        final Iterator<String> iterator = internalNewsFeedMap.keySet().iterator();
        int i = 0;
        while (iterator.hasNext()) {
            data.put(i, internalNewsFeedMap.get(iterator.next()));
            i++;
        }


        return data;

    }
}
