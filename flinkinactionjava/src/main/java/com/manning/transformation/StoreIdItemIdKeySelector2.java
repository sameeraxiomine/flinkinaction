package com.manning.transformation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import com.manning.model.petstore.TransactionItem;

public class StoreIdItemIdKeySelector2 implements KeySelector<TransactionItem, Tuple2<Integer,Integer>>{
    @Override
    public Tuple2<Integer,Integer> getKey(TransactionItem value) throws Exception {        
        return new Tuple2<Integer,Integer>(value.storeId,value.itemId);
    }


    
}
