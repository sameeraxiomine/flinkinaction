package com.manning.fia.c03;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

import com.manning.fia.transformations.ComputeTransactionValue;
import com.manning.fia.transformations.TransactionItemParser;

public class SimpleMapOperatorPseudoClusterClient {

    public static void main(String[] args) throws Exception {
        String[] transactionItemLines = {                  //#A
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
                "1006,7,3,3_item,7,200.0," + "20151231150000"
        };
        /*
         * Create an Execution Environment using a Job Manager URL - localhost:6123
         * Ship the JAR file to Job Manager which will in turn ship it to 
         * each task manager where the task is allocated
         */
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createRemoteEnvironment("localhost",6123,
                                 "target/flinkinactionjava-0.0.1-SNAPSHOT.jar");
        //Set the parallelism to 5
        execEnv.setParallelism(5);
        //Create a DataSet from the local collection
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(transactionItemLines));
        
       //Parse each line (record) into a Tuple7 instance
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
                .map(new TransactionItemParser());

        //Repartition by first attribute
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> partitionedTuples = tuples
                .partitionByHash(0);
        
        //Apply the transformation Map Operator to create the new attribute and select
        //the required attributes from the original record
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = partitionedTuples
                .map(new ComputeTransactionValue());

        //Execute job and collect the results
        List<Tuple5<Integer, Long, Integer, String, Double>> list = transformedTuples.collect();
        for(Tuple5<Integer, Long, Integer, String, Double> record:list){
            System.out.println(record);
        }
    }
}
