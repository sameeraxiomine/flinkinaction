package com.manning.transformation;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import com.manning.model.petstore.TransactionItem;
public final class SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId
        implements
        GroupReduceFunction<Tuple3<Integer, Integer, Double>,Tuple3<Integer,Integer, Double>> {
    

    @Override
    public void reduce(Iterable<Tuple3<Integer, Integer, Double>> values,
            Collector<Tuple3<Integer, Integer, Double>> out) throws Exception {
        int storeId=-1;
        int itemId=-1;
        double total = 0d;
        for(Tuple3<Integer, Integer, Double> value:values){
            storeId = value.f0;
            if(itemId<0 || itemId!=value.f1){                
                if(itemId>0){//Collect the previous value
                    out.collect(new Tuple3<Integer,Integer,Double>(storeId,itemId,total));
                }
                itemId = value.f1;
                total = value.f2;
            }else{
                total = total + value.f2;
            }   
        }
        if(storeId>0 && itemId>0){//Collect the last value if valid store and item
            out.collect(new Tuple3<Integer,Integer,Double>(storeId,itemId,total));
        }
             
    }
}