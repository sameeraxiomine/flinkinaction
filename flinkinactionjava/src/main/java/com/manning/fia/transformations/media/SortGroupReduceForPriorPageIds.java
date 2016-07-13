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
        String>, Tuple4<String, Long, String, List<Long>>> {
    @Override
    public void reduce(Iterable<Tuple4<Long, Long, Long, String>> values,
                       Collector<Tuple4<String, Long, String, List<Long>>> out) throws Exception {

        List<Long> priorPageIds = new ArrayList<Long>();
        Long startTimeStamp = null;
        String userId = null;
        String ds = null;
        Long parentPageId = null;
        for (Tuple4<Long, Long, Long, String> value : values) {
            if (userId == null) {
                userId = value.f3;
            }
            if (startTimeStamp == null) {
                startTimeStamp = value.f1;
                ds = DateTimeFormat.forPattern("yyyyMMddHHmmss").print(startTimeStamp);
            }
            long pageId = value.f0;
            if (parentPageId == null) {
                parentPageId = pageId;
            } else {
                priorPageIds.add(pageId);
            }
        }
        out.collect(new Tuple4<>(userId, parentPageId, ds, priorPageIds));
    }
}
