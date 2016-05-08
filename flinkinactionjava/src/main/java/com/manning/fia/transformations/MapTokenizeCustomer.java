package com.manning.fia.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

@SuppressWarnings("serial")
public  class MapTokenizeCustomer implements
        MapFunction<String, Tuple3<Integer, String,String>> {
    @Override
    public Tuple3<Integer, String,String> map(String value)
            throws Exception {
        String[] tokens = value.toLowerCase().split(",");
        int customerId = Integer.parseInt(tokens[0]);
        String customerName = tokens[1];
        String zipcode = tokens[2];
        return new Tuple3<>(customerId, customerName, zipcode);
    }
}