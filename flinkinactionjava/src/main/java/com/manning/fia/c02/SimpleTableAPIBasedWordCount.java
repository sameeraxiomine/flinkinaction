package com.manning.fia.c02;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;

import java.util.Arrays;


public class SimpleTableAPIBasedWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(1);
        String[] lines = { "20160301120100,#DCFlinkMeetup",
                "20160301120200,#DcFlinkMeetup", "20160301120300,#Flink",
                "20160301130200,#Flink", "20160301130200,#DCFlinkMeetup" };
        BatchTableEnvironment tblEnv = TableEnvironment.getTableEnvironment(execEnv);
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(lines));

        DataSet<Tuple3<String, String, Integer>> intermediateDS = source
                .map(new SimpleBatchWordCount.Tokenizer());
        Table table = tblEnv.fromDataSet(intermediateDS, "datetime,word,index");
        Table output = table.groupBy("datetime,word")
                .select("datetime,word, index.sum as wrdCnt")
                .filter("wrdCnt>1");
        tblEnv.toDataSet(output, Row.class).print();
    }
}
