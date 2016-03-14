package com.manning.chapter2;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Table;

import com.manning.chapter2.TableAPIWordCount.Word;

public class SimpleTableAPIBasedWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
        String[] lines = { "201603011201,#DCFlinkMeetup",
                "201603011202,#DcFlinkMeetup", "201603011203,#Flink",
                "201603011302,#Flink", "201603011302,#DCFlinkMeetup" };
        TableEnvironment tblEnv = new TableEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(lines));
        
        
        DataSet<Tuple3<String, String, Integer>> intermediateDS = source.map(new SimpleBatchWordCount.Tokenizer());
        Table table = tblEnv.fromDataSet(intermediateDS,
                "datetime,word,index");
        Table output = table.groupBy("datetime,word").select(
                "datetime,word, index.sum as wrdCnt");
        DataSet<Word> result = tblEnv.toDataSet(output, TableAPIWordCount.Word.class);
        result.print();

    }

}
