package com.manning.fia.transformations.media;

import com.manning.fia.utils.DateUtils;
import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class MapPartitionNewsFeedMapper
        implements
        MapPartitionFunction<String, Tuple5<Long, String, String, String, Long>> {
    private DateUtils dateUtils = new DateUtils();
    @Override
    public void mapPartition(Iterable<String> values,
            Collector<Tuple5<Long, String, String, String, Long>> out)
            throws Exception {
        for (String value : values) {
            out.collect(mymap(value));
        }
    }

    private Tuple5<Long, String, String, String, Long> mymap(final String value)
            throws Exception {
        NewsFeed newsFeed = NewsFeedParser.mapRow(value);
        long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);
        return  new Tuple5<>(
                    newsFeed.getPageId(),
                    newsFeed.getSection(),
                    newsFeed.getSubSection(), 
                    newsFeed.getTopic(), 
                    timeSpent);
        }
}