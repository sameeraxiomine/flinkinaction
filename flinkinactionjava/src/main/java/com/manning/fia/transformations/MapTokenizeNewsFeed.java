package com.manning.fia.transformations;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class MapTokenizeNewsFeed implements MapFunction<String, Tuple3<String, String, Integer>> {
    @Override
    public Tuple3<String, String, Integer> map(
            String value) throws Exception {

        final NewsFeed newsFeed = NewsFeedParser.map(value);
        final String section = newsFeed.getPageInfo().f0;
        final String subsection = newsFeed.getPageInfo().f1;
        final Tuple3<String,String,Integer> sectionSubSectionCount=new Tuple3<>(section,subsection,1);
        return sectionSubSectionCount;
    }
}
