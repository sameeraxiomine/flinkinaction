package com.manning.fia.transformations;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.GroupReduceFunction;

public class SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId
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
        out.collect(new Tuple3<Integer,Integer,Double>(storeId,itemId,total));
    }
}