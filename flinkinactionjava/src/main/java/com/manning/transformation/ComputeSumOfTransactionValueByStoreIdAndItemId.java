package com.manning.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
public final class ComputeSumOfTransactionValueByStoreIdAndItemId
        implements
        ReduceFunction<Tuple3<Integer,Integer, Double>> {
    
    @Override
    public Tuple3<Integer, Integer, Double> reduce(
            Tuple3<Integer, Integer, Double> value1,
            Tuple3<Integer, Integer, Double> value2) throws Exception {
        return new Tuple3<>(value1.f0,value1.f1,value1.f2+value2.f2);        
    }
}