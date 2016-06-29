package com.manning.fia.transformations.media;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

@SuppressWarnings("serial")
public class NewsFeedMapper3 implements MapFunction<String, Tuple5<Long, String, String, String, String>> {
    @Override
    public Tuple5<Long, String, String, String, String> map(String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);

        Tuple5<Long, String, String, String, String> tuple6 = new Tuple5<>(
                                                                        newsFeed.getEventId(),
                                                                        newsFeed.getSection(),
                                                                        newsFeed.getSubSection(),
                                                                        newsFeed.getStartTimeStamp(),
                                                                        newsFeed.getEndTimeStamp()
        );
        return tuple6;
    }
}

