package com.manning.fia.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

@SuppressWarnings("serial")
public class MapTokenizeTransaction implements
        MapFunction<String, Tuple4<Integer, Long, Integer, Long>> {
    @Override
    public Tuple4<Integer, Long, Integer, Long> map(String value)
            throws Exception {
        String[] tokens = value.toLowerCase().split(",");
        int storeId = Integer.parseInt(tokens[0]);
        long transactionId = Long.parseLong(tokens[1]);
        int customerId = Integer.parseInt(tokens[2]);
        long time = Long.parseLong(tokens[3]);
        return new Tuple4<>(storeId, transactionId, customerId, time);
    }
}
