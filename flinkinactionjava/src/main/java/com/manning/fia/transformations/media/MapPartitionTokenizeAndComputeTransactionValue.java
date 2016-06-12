package com.manning.fia.transformations.media;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class MapPartitionTokenizeAndComputeTransactionValue implements
        MapPartitionFunction<NewsFeed, Tuple4<String, String, String, Long>> {

    @Override
    public void mapPartition(Iterable<NewsFeed> values,
                             Collector<Tuple4<String, String, String, Long>> out)
            throws Exception {
        for (NewsFeed value : values) {
            out.collect(map(value));
        }
    }

    private Tuple4<String, String, String, Long> map(final NewsFeed newsFeed) throws Exception {

        final long startTime = newsFeed.getStartTimeStamp();
        final long endTime = newsFeed.getEndTimeStamp();
        final long timeSpent = endTime - startTime;

        final Tuple4<String, String, String, Long> timeSpentForSectionAndSubSection = new Tuple4<>(newsFeed.getSection(),
                newsFeed.getSubSection(), newsFeed.getTopic(), timeSpent);
        return timeSpentForSectionAndSubSection;
    }
}