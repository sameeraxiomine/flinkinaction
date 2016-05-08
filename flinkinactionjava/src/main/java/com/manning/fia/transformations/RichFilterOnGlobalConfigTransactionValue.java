package com.manning.fia.transformations;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
@SuppressWarnings("serial")
public class RichFilterOnGlobalConfigTransactionValue
        extends
        RichFilterFunction<Tuple5<Integer, Long, Integer, String, Double>> {
    private int criteria;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globConf = (Configuration) globalParams;                
        criteria = globConf.getInteger("gtTransactionValue", 0);
    }
    @Override
    public boolean filter(
            Tuple5<Integer, Long, Integer, String, Double> transactionItem)
            throws Exception {
        return transactionItem.f4>criteria;
    }
}