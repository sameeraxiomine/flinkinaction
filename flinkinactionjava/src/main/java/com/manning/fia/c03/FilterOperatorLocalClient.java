package com.manning.fia.c03;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;

import com.manning.fia.transformations.ComputeTransactionValue;
import com.manning.fia.transformations.FilterOnTransactionValue;
import com.manning.fia.transformations.RichFilterOnGlobalConfigTransactionValue;
import com.manning.fia.transformations.RichFilterOnTransactionValue;
import com.manning.fia.transformations.TransactionItemParser;

public class FilterOperatorLocalClient {

    public static void withGlobalConfigExample(List<String> transactionItemLines)
            throws Exception {
        
        Configuration conf = new Configuration();
        conf.setInteger("gtTransactionValue", 500);        
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment();
        execEnv.getConfig().setGlobalJobParameters(conf);
        
        DataSet<String> source = execEnv.fromCollection(transactionItemLines);
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
                .map(new TransactionItemParser());
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = tuples
                .map(new ComputeTransactionValue());


        DataSet<Tuple5<Integer, Long, Integer, String, Double>> filteredTuples = transformedTuples
                .filter(new RichFilterOnGlobalConfigTransactionValue());

        List<Tuple5<Integer, Long, Integer, String, Double>> output = filteredTuples
                .collect();
        for (Tuple5<Integer, Long, Integer, String, Double> line : output) {
            System.out.println(line);
        }

    }

    
    public static void withConfigExample(List<String> transactionItemLines)
            throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment();
        DataSet<String> source = execEnv.fromCollection(transactionItemLines);
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
                .map(new TransactionItemParser());
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = tuples
                .map(new ComputeTransactionValue());

        Configuration conf = new Configuration();
        conf.setInteger("gtTransactionValue", 500);
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> filteredTuples = transformedTuples
                .filter(new RichFilterOnTransactionValue())
                .withParameters(conf);

        List<Tuple5<Integer, Long, Integer, String, Double>> output = filteredTuples
                .collect();
        for (Tuple5<Integer, Long, Integer, String, Double> line : output) {
            System.out.println(line);
        }

    }

    public static void withHardcodedFilter(List<String> transactionItemLines) throws Exception{
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment();
        DataSet<String> source = execEnv.fromCollection(transactionItemLines);
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
                .map(new TransactionItemParser());
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = tuples
                .map(new ComputeTransactionValue()).filter(
                        new FilterOnTransactionValue());
        List<Tuple5<Integer, Long, Integer, String, Double>> output = transformedTuples
                .collect();
        for (Tuple5<Integer, Long, Integer, String, Double> line : output) {
            System.out.println(line);
        }
    }

    public static void main(String[] args) throws Exception {
        String[] transactionItemLines = {
                "1000,1,1,1_item,5,1.0," + "20151231130000",
                "1000,1,2,2_item,10,100.0," + "20151231130000",
                "1000,1,3,3_item,3,200.0," + "20151231130000",
                "1001,2,1,1_item,4,1.0," + "20151231130000",
                "1001,2,2,2_item,11,100.0," + "20151231130000",
                "1001,2,3,3_item,7,200.0," + "20151231130000",
                "1002,3,1,1_item,4,1.0," + "20151231130000",
                "1002,3,2,2_item,11,100.0," + "20151231130000",
                "1002,3,3,3_item,7,200.0," + "20151231130000",
                "1003,4,1,1_item,4,1.0," + "20151231150000",
                "1003,4,2,2_item,11,100.0," + "20151231150000",
                "1003,4,3,3_item,7,200.0," + "20151231150000",
                "1004,5,1,1_item,4,1.0," + "20151231150000",
                "1004,5,2,2_item,11,100.0," + "20151231150000",
                "1004,5,3,3_item,7,200.0," + "20151231150000",
                "1005,6,1,1_item,4,1.0," + "20151231150000",
                "1005,6,2,2_item,11,100.0," + "20151231150000",
                "1005,6,3,3_item,7,200.0," + "20151231150000",
                "1006,7,1,1_item,4,1.0," + "20151231150000",
                "1006,7,2,2_item,11,100.0," + "20151231150000",
                "1006,7,3,3_item,7,200.0," + "20151231150000" };

        FilterOperatorLocalClient.withHardcodedFilter(Arrays
                .asList(transactionItemLines));

        
        FilterOperatorLocalClient.withConfigExample(Arrays
                .asList(transactionItemLines));

        FilterOperatorLocalClient.withGlobalConfigExample(Arrays
                .asList(transactionItemLines));

        
    }
}
