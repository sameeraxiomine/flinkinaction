package com.manning.fia.transformations.media;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import com.manning.fia.model.media.NewsFeed;

@SuppressWarnings("serial")
public class SectionSubSectionKeySelector implements KeySelector<NewsFeed, Tuple2<String, String>>{
    @Override
    public Tuple2<String, String> getKey(NewsFeed value)
            throws Exception {
        return new Tuple2<>(value.getSection(), value
                .getSubSection());
    }    
}
