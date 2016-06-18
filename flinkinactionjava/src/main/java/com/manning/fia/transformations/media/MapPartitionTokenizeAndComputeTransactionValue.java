package com.manning.fia.transformations.media;

import com.manning.fia.c03.media.DateUtils;
import com.manning.fia.model.media.NewsFeed;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class MapPartitionTokenizeAndComputeTransactionValue implements
        MapPartitionFunction<String, Tuple4<String, String, String, Long>> {
    private DateUtils dateUtils = new DateUtils();
    @Override
    public void mapPartition(Iterable<String> values,
                             Collector<Tuple4<String, String, String, Long>> out)
            throws Exception {
        for (String value : values) {
            out.collect(map(value));
        }
    }

    private Tuple4<String, String, String, Long> map(final String value) throws Exception {

        NewsFeed newsFeed=NewsFeedParser.mapRow(value);
        long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);

        Tuple4<String, String, String, Long> timeSpentForSectionAndSubSection = new Tuple4<>(newsFeed.getSection(),
                newsFeed.getSubSection(), newsFeed.getTopic(), timeSpent);
        return timeSpentForSectionAndSubSection;
    }
}