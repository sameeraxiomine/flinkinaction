package com.manning.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

import com.manning.model.petstore.TransactionItem;

public final class DomainObjectBasedMap implements
        MapFunction<String, TransactionItem> {
    @Override
    public TransactionItem map(String value) throws Exception {
        String[] tokens = value.toLowerCase().split(",");
        int storeId = Integer.parseInt(tokens[0]);
        long transactionId = Long.parseLong(tokens[1]);
        int itemId = Integer.parseInt(tokens[2]);
        String itemDesc = tokens[3];
        int itemQty = Integer.parseInt(tokens[4]);
        double pricePerItem = Double.parseDouble(tokens[5]);
        long time = Long.parseLong(tokens[6]);
        return new TransactionItem(storeId, transactionId, itemId, itemDesc,
                 itemQty, pricePerItem, time);
    }
}