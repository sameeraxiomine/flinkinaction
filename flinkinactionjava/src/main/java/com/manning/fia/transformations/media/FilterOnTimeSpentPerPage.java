package com.manning.fia.transformations.media;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
@SuppressWarnings("serial")
public class FilterOnTimeSpentPerPage
        implements
        FilterFunction<Tuple5<Long,String, String, String, Long>> {
    @Override
    public boolean filter(
            Tuple5<Long,String, String, String, Long> tuple5)
            throws Exception {
        return tuple5.f4 > 60000;
    }
}