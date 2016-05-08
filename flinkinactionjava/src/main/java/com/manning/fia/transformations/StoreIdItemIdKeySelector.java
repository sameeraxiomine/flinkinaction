package com.manning.fia.transformations;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import com.manning.fia.model.petstore.TransactionItem;

@SuppressWarnings("serial")
public class StoreIdItemIdKeySelector implements KeySelector<TransactionItem, Tuple2<Integer,Integer>>{
    @Override
    public Tuple2<Integer,Integer> getKey(TransactionItem value) throws Exception {        
        return new Tuple2<Integer,Integer>(value.storeId,value.itemId);
    }


    
}
