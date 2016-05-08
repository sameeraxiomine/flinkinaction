package com.manning.fia.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;

@SuppressWarnings("serial")
public class TransactionItemParser
        implements
        MapFunction<String, Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> {
    @Override
    public Tuple7<Integer, Long, Integer, String, Integer, Double, Long> map(
            String value) throws Exception {
        String[] tokens = value.toLowerCase().split(",");
        int storeId = Integer.parseInt(tokens[0]);
        long transactionId = Long.parseLong(tokens[1]);
        int itemId = Integer.parseInt(tokens[2]);
        String itemDesc = tokens[3];
        int itemQty = Integer.parseInt(tokens[4]);
        double pricePerItem = Double.parseDouble(tokens[5]);
        long timestamp = Long.parseLong(tokens[6]);
        return new Tuple7<>(storeId, transactionId, itemId, itemDesc, itemQty,
                pricePerItem, timestamp);
    }
}
