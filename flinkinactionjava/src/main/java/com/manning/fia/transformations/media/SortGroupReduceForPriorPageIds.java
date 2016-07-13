package com.manning.fia.transformations.media;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 7/13/16.
 */
public class SortGroupReduceForPriorPageIds implements GroupReduceFunction<Tuple4<Long, Long, Long,
        String>, Tuple4<String, Long, String, Long>> {
    @Override
    public void reduce(Iterable<Tuple4<Long, Long, Long, String>> values,
                       Collector<Tuple4<String, Long, String,Long>> out) throws Exception {

        
        Long startTimeStamp = null;
        String userId = null;
        String ds = null;
        Long parentPageId = 0l;
        Long priorPageId = 0l;
        for (Tuple4<Long, Long, Long, String> value : values) {
            if (userId == null) {
                userId = value.f3;
            }
            startTimeStamp = value.f1;
            ds = DateTimeFormat.forPattern("yyyyMMddHHmmss").print(startTimeStamp);
            long pageId = value.f0;
            out.collect(new Tuple4<>(userId, parentPageId, ds, priorPageId));
            priorPageId = pageId;
        }        
    }
}
