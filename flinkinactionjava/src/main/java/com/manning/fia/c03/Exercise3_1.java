package com.manning.fia.c03;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

import com.manning.transformation.ComputeTransactionValue;
import com.manning.transformation.MapTokenizeAndComputeTransactionValue;
import com.manning.transformation.TransactionItemParser;

public class Exercise3_1 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = source
                .map(new MapTokenizeAndComputeTransactionValue());
        List<Tuple5<Integer, Long, Integer, String, Double>> output = transformedTuples
                .collect();
        for (Tuple5<Integer, Long, Integer, String, Double> line : output) {
            System.out.println(line);
        }

    }
}
