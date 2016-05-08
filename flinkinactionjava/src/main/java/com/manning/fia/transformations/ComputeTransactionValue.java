package com.manning.fia.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
@SuppressWarnings("serial")
public class ComputeTransactionValue
        implements
        MapFunction<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>, Tuple5<Integer, Long, Integer, String, Double>> {
    @Override
    public Tuple5<Integer, Long, Integer, String, Double> map(
            Tuple7<Integer, Long, Integer, String, Integer, Double, Long> transactionItem)
            throws Exception {
        int itemQty = transactionItem.f4;
        double pricePerItem = transactionItem.f5;
        double transactionValue = itemQty * pricePerItem;
        return new Tuple5<>(transactionItem.f0, transactionItem.f1,
                transactionItem.f2, transactionItem.f3, transactionValue);
    }
}