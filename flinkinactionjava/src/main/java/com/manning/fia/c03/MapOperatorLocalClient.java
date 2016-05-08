package com.manning.fia.c03;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.joda.time.DateTime;

import com.manning.fia.transformations.ComputeTransactionValue;
import com.manning.fia.transformations.MapTokenizeAndComputeTransactionValue;
import com.manning.fia.transformations.TransactionItemParser;

public class MapOperatorLocalClient {

    public void usingTokenizeFollowedByTransformerAfterPartitioning(
            List<String> transactionItemLines) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createRemoteEnvironment("localhost",6123,"target/flinkinactionjava-0.0.1-SNAPSHOT.jar");
        execEnv.setParallelism(4);
        DataSet<String> source = execEnv.fromCollection(transactionItemLines);        
        
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
                .map(new TransactionItemParser());

        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> partitionedTuples = tuples
                .partitionByHash(0);
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = partitionedTuples
                .map(new ComputeTransactionValue());
        
        String outputFolderPath = "./src/main/resources/petstore/output/c031";
        File outputFolder =  new File(outputFolderPath);
        FileUtils.deleteQuietly(outputFolder);
        transformedTuples.writeAsText(outputFolder.getAbsolutePath());
        execEnv.execute();
    }

    public void usingTokenizeFollowedByTransformer(
            List<String> transactionItemLines) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(transactionItemLines);
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
                .map(new TransactionItemParser());
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = tuples
                .map(new ComputeTransactionValue());
        List<Tuple5<Integer, Long, Integer, String, Double>> output = transformedTuples
                .collect();
        for (Tuple5<Integer, Long, Integer, String, Double> line : output) {
            System.out.println(line);
        }
    }

    public void usingTokenizeTransformer(List<String> transactionItemLines)
            throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(transactionItemLines);
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = source
                .map(new MapTokenizeAndComputeTransactionValue());
        List<Tuple5<Integer, Long, Integer, String, Double>> output = transformedTuples
                .collect();
        for (Tuple5<Integer, Long, Integer, String, Double> line : output) {
            System.out.println(line);
        }
    }

    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        Date tDt = sdf.parse("20151231130000");
        DateTime tDtTime1 = new DateTime(tDt);
        DateTime tDtTime2 = tDtTime1.plusHours(2);
        String tDtTime1Str = sdf.format(tDtTime1.toDate());
        String tDtTime2Str = sdf.format(tDtTime2.toDate());
        String[] transactionItemLines = {
                "1000,1,1,1_item,5,1.0," + tDtTime1Str,
                "1000,1,2,2_item,10,100.0," + tDtTime1Str,
                "1000,1,3,3_item,3,200.0," + tDtTime1Str,
                "1001,2,1,1_item,4,1.0," + tDtTime1Str,
                "1001,2,2,2_item,11,100.0," + tDtTime1Str,
                "1001,2,3,3_item,7,200.0," + tDtTime1Str,
                "1002,3,1,1_item,4,1.0," + tDtTime1Str,
                "1002,3,2,2_item,11,100.0," + tDtTime1Str,
                "1002,3,3,3_item,7,200.0," + tDtTime1Str,
                "1003,4,1,1_item,4,1.0," + tDtTime2Str,
                "1003,4,2,2_item,11,100.0," + tDtTime2Str,
                "1003,4,3,3_item,7,200.0," + tDtTime2Str,
                "1004,5,1,1_item,4,1.0," + tDtTime2Str,
                "1004,5,2,2_item,11,100.0," + tDtTime2Str,
                "1004,5,3,3_item,7,200.0," + tDtTime2Str,
                "1005,6,1,1_item,4,1.0," + tDtTime2Str,
                "1005,6,2,2_item,11,100.0," + tDtTime2Str,
                "1005,6,3,3_item,7,200.0," + tDtTime2Str,
                "1006,7,1,1_item,4,1.0," + tDtTime2Str,
                "1006,7,2,2_item,11,100.0," + tDtTime2Str,
                "1006,7,3,3_item,7,200.0," + tDtTime2Str
        };

        MapOperatorLocalClient client = new MapOperatorLocalClient();
        client.usingTokenizeFollowedByTransformer(Arrays
                .asList(transactionItemLines));

        client.usingTokenizeTransformer(Arrays.asList(transactionItemLines));

        client.usingTokenizeFollowedByTransformerAfterPartitioning(Arrays
                .asList(transactionItemLines));
        
    }





}
