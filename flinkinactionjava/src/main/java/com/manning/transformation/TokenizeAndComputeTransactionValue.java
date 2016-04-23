package com.manning.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

public final class TokenizeAndComputeTransactionValue implements
        MapFunction<String, Tuple5<Integer, Long, Integer, String, Double>> {
    @Override
    public Tuple5<Integer, Long, Integer, String, Double> map(String value)
            throws Exception {
        String[] tokens = value.toLowerCase().split(",");
        int customerId = Integer.parseInt(tokens[0]);
        long transactionId = Long.parseLong(tokens[1]);
        int itemId = Integer.parseInt(tokens[2]);
        String itemDesc = tokens[3];
        int itemQty = Integer.parseInt(tokens[4]);
        double pricePerItem = Double.parseDouble(tokens[5]);
        long timestamp = Long.parseLong(tokens[6]);
        return new Tuple5<>(customerId, transactionId, itemId, itemDesc,
                itemQty * pricePerItem);
    }
}