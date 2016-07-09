package com.manning.fia.c02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;
import java.util.List;

public class SimpleBatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment execEnv =
                ExecutionEnvironment.createLocalEnvironment(1);

        String filePath="/tmp/tmp.csv";
        
        String[] lines = {"201603011201,#DCFlinkMeetup",
                "201603011202,#DcFlinkMeetup",
                "201603011203,#Flink",
                "201603011302,#Flink",
                "201603011302,#DCFlinkMeetup"};
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(lines));

        DataSet<Tuple3<String, String, Integer>> counts =
                source.map(new Tokenizer())
                        .groupBy(0, 1)
                        .sum(2);

        List<Tuple3<String, String, Integer>> output = counts.collect();
        for (Tuple3<String, String, Integer> line : output) {
            System.out.println(line.f0 + "," + line.f1 + "," + line.f2);
        }

        // Alternative 1 to counts.collect()
//         counts.print();

        //Alternative 2 to counts.collect()
//         counts.printToErr();

        //Alternative 3 to counts.collect()
//        counts.writeAsCsv(filePath, "\n", ",");
        execEnv.execute();
    }

    @SuppressWarnings("serial")
    public static final class Tokenizer implements
            MapFunction<String, Tuple3<String, String, Integer>> {
        @Override
        public Tuple3<String, String, Integer> map(String value)
                throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            String newDt = tokens[0].substring(0, 10);
            String word = tokens[1].toLowerCase();
            return new Tuple3<>(newDt, word, 1);
        }
    }
}
