package com.manning.chapter2;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class SimpleStreamWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
        String[] lines = { "20160301120100,#DCFlinkMeetup",
                "20160301120200,#DcFlinkMeetup", "20160301120300,#Flink",
                "20160301130200,#Flink", "20160301130200,#DCFlinkMeetup" };
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(lines));
        
        DataSet<Tuple3<String, String, Integer>> counts = source.map(new Tokenizer())
                                                                .groupBy(0, 1)
                                                                .sum(2);
        List<Tuple3<String,String,Integer>> output = counts.collect();
        for (Tuple3<String,String,Integer> line : output) {
            System.out.println(line.f0 +","+line.f1 + ","+line.f2);
        }
        counts.print();
        
        /*Write to a file*/
        //counts.writeAsCsv(filePath, "\n", ",");
        //execEnv.execute();
    }

    @SuppressWarnings("serial")
    public static final class Tokenizer implements
            MapFunction<String, Tuple3<String, String, Integer>> {
        @Override
        public Tuple3<String, String, Integer> map(String value)
                throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            String newDt = tokens[0].substring(0,10);
            String word = tokens[1].toLowerCase();
            return new Tuple3<>(newDt, word, 1);
        }
    }
}
