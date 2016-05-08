package com.manning.fia.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public final class MapTokenizeStore implements
        MapFunction<String, Tuple2<Integer, String>> {
    @Override
    public Tuple2<Integer, String> map(String value)
            throws Exception {
        String[] tokens = value.toLowerCase().split(",");
        int storeId = Integer.parseInt(tokens[0]);
        String zipcode = tokens[1];
        return new Tuple2<>(storeId, zipcode);
    }
}