package com.manning.fia.transformations.media;

import com.manning.fia.c03.media.DateUtils;
import com.manning.fia.model.media.NewsFeed;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.functions.GroupReduceFunction;

@SuppressWarnings("serial")
public final class GroupReduceComputeTimeSpentPerSectionAndSubSection implements
        GroupReduceFunction<NewsFeed, Tuple3<String, String, Long>> {
    private DateUtils dateUtils = new DateUtils();
    @Override
    public void reduce(Iterable<NewsFeed> newsFeeds,
            Collector<Tuple3<String, String, Long>> out) throws Exception {
        long total = 0l;
        String section = null;
        String subSection = null;
        for (NewsFeed feed : newsFeeds) {
            section = feed.getSection();
            subSection = feed.getSubSection();
            final long timeSpent = dateUtils.getTimeSpentOnPage(feed);
            total = total + timeSpent;
        }
        out.collect(new Tuple3<>(section, subSection, total));
    }
}