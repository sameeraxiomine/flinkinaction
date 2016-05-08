package com.manning.fia.transformations;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class MapPartitionTokenizeAndComputeTransactionValue implements
        MapPartitionFunction<String, Tuple5<Integer, Long, Integer, String, Double>> {    

    @Override
    public void mapPartition(Iterable<String> values,
            Collector<Tuple5<Integer, Long, Integer, String, Double>> out)
            throws Exception {
        for(String value:values){
            out.collect(map(value));
        }        
    }
    
    private Tuple5<Integer, Long, Integer, String, Double> map(String value){
        String[] tokens = value.toLowerCase().split(",");
        int storeId = Integer.parseInt(tokens[0]);
        long transactionId = Long.parseLong(tokens[1]);
        int itemId = Integer.parseInt(tokens[2]);
        String itemDesc = tokens[3];
        int itemQty = Integer.parseInt(tokens[4]);
        double pricePerItem = Double.parseDouble(tokens[5]);
        return new Tuple5<>(storeId, transactionId, itemId, itemDesc,
                itemQty * pricePerItem);
    }
}