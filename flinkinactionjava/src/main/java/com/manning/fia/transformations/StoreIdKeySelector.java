package com.manning.fia.transformations;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import com.manning.fia.model.petstore.TransactionItem;

@SuppressWarnings("serial")
public class StoreIdKeySelector implements KeySelector<TransactionItem, Tuple1<Integer>>{
    @Override
    public Tuple1<Integer> getKey(TransactionItem value) throws Exception {        
        return new Tuple1<Integer>(value.storeId);
    }


    
}
