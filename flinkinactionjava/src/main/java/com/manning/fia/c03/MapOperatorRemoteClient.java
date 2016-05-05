package com.manning.fia.c03;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;

import com.manning.transformation.ComputeTransactionValue;
import com.manning.transformation.TransactionItemParser;

public class MapOperatorRemoteClient {
   
    public static void main(String[] args) throws Exception {    
        ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        execEnv.getConfig().setGlobalJobParameters(params);
        System.out.println("Usage: MapOperatorRemoteClient --input <path> --output <path>");        
        DataSet<String> source = execEnv.readTextFile(params.get("input"));
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
            .map(new TransactionItemParser());
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = tuples
            .map(new ComputeTransactionValue());
        transformedTuples.writeAsCsv(params.get("output"), "\n", ",");
        execEnv.execute();
        
        
    }    
    
}
