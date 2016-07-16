package com.manning.fia.transformations.media;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 7/13/16.
 */
@SuppressWarnings("serial")
public class SortGroupReduceForPriorPageId implements GroupReduceFunction<Tuple3<String,Long, Long>, Tuple4<String, String,Long, Long>> {

    @Override
    public void reduce(Iterable<Tuple3<String,Long, Long>> values,
            Collector<Tuple4<String, String, Long, Long>> out) throws Exception {
     
        Long startTimeStamp = null;
        String userId = null;
        String accessTime = null;
        Long priorPageId = 0l;
        for (Tuple3<String,Long, Long> value : values) {
            if (userId == null) {
                userId = value.f0;
            }
            long pageId = value.f1;
            startTimeStamp = value.f2;
            accessTime = DateTimeFormat.forPattern("yyyyMMddHHmmss").print(startTimeStamp);
            out.collect(new Tuple4<>(userId, accessTime,pageId,priorPageId));
            priorPageId = pageId;
        }     
    }
}
