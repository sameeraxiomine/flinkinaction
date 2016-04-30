package com.manning.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public final class MapTokenizeAndComputeTransactionValue2 implements
        MapFunction<String, Tuple6<Integer, Long, Integer, Integer,Double,Double>> {
    @Override
    public Tuple6<Integer, Long, Integer, Integer,Double,Double> map(String value)
            throws Exception {
        String[] tokens = value.toLowerCase().split(",");
        int storeId = Integer.parseInt(tokens[0]);
        long transactionId = Long.parseLong(tokens[1]);
        int itemId = Integer.parseInt(tokens[2]);        
        int itemQty = Integer.parseInt(tokens[4]);
        double pricePerItem = Double.parseDouble(tokens[5]);
        return new Tuple6<>(storeId, transactionId, itemId, itemQty,pricePerItem,
                itemQty * pricePerItem);
    }
}