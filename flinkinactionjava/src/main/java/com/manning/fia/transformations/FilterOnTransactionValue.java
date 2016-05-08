package com.manning.fia.transformations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
@SuppressWarnings("serial")
public class FilterOnTransactionValue
        implements
        FilterFunction<Tuple5<Integer, Long, Integer, String, Double>> {
    @Override
    public boolean filter(
            Tuple5<Integer, Long, Integer, String, Double> transactionItem)
            throws Exception {
        return transactionItem.f4>1000;
    }
}