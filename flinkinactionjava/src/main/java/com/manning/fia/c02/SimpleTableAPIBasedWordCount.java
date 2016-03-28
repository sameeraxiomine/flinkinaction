package com.manning.fia.c02;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.table.Table;

import com.manning.fia.c02.TableAPIWordCount.Word;

public class SimpleTableAPIBasedWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(1);
        String[] lines = { "20160301120100,#DCFlinkMeetup",
                "20160301120200,#DcFlinkMeetup", "20160301120300,#Flink",
                "20160301130200,#Flink", "20160301130200,#DCFlinkMeetup" };
        TableEnvironment tblEnv = new TableEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(lines));

        DataSet<Tuple3<String, String, Integer>> intermediateDS = source
                .map(new SimpleBatchWordCount.Tokenizer());
        Table table = tblEnv.fromDataSet(intermediateDS, "datetime,word,index");
        Table output = table.groupBy("datetime,word")
                .select("datetime,word, index.sum as wrdCnt")
                .filter("wrdCnt>1");
        DataSet<Word> result = tblEnv.toDataSet(output, Word.class);
        result.print();

    }

    public static class Word {
        // empty constructor to satisfy POJO requirements
        public Word() {
        }

        public String datetime;
        public String word;
        public int wrdCnt;
        
        @Override
        public String toString() {
            return datetime+","+word+","+wrdCnt;
        }
    }
}
