package com.manning.fia.transformations.media;


import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class SortedGroupReduceComputeTimeSpentBySectionAndSubSection
        implements
        GroupReduceFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>> {
    @Override
    public void reduce(
            Iterable<Tuple3<String, String, Long>> newsFeeds,
            Collector<Tuple3<String, String, Long>> out)
            throws Exception {
        long total = 0l;
        String section = null;
        String subSection = null;
        for (Tuple3<String, String, Long> feed : newsFeeds) {
            section = feed.f0;            
            if(subSection==null){
                subSection = feed.f1;    
            }else if(!subSection.equals(feed.f1)){
                out.collect(new Tuple3<>(section, subSection,total));
                subSection = feed.f1;
                total = 0;
            }            
            total = total + feed.f2;
        }
        if(subSection!=null){
            //Collect last result
            out.collect(new Tuple3<>(section, subSection,total));
        }
    }

}