package com.manning.chapter2;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.manning.utils.datagen.HashTagGenerator;
import com.manning.utils.datagen.IDataGenerator;

public class BatchWordCountTest {
    public static String SAMPLE_INPUT_FILE_NAME = "hashtags.txt";
    public static String SAMPLE_INPUT_PATH = "src/test/resources/input/";
    public static String SAMPLE_OUTPUT_PATH = "src/test/resources/output/counts";
    public static String[] lines = { "201603011201,#DCFlinkMeetup",
            "201603011202,#DcFlinkMeetup", "201603011203,#Flink",
            "201603011302,#Flink", "201603011302,#DCFlinkMeetup" };

    private IDataGenerator<String> dataGenerator = null;

    @Before
    public  void setup() throws IOException {
        // Delete Input/Output Folder if exists
        this.cleanup();
        this.dataGenerator = new HashTagGenerator();
        dataGenerator.setData(Arrays.asList(lines));
        FileUtils.writeLines(
                new File(SAMPLE_INPUT_PATH, SAMPLE_INPUT_FILE_NAME),
                Arrays.asList(lines));
    }

    @After
    public  void cleanup() throws IOException {
        FileUtils.deleteQuietly(new File(SAMPLE_INPUT_PATH));
        FileUtils.deleteQuietly(new File(SAMPLE_OUTPUT_PATH));
    }

    @Test
    public void testExecuteJobWithTestData() {
        BatchWordCount wordCountJob = new BatchWordCount(new String[0]);
        wordCountJob.setDataGenerator(dataGenerator);
        wordCountJob.initializeExecutionEnvironment(ExecutionEnvironment.createLocalEnvironment(1));
        wordCountJob.executeJob();        
        List<Tuple3<String, String, Integer>> outputList = wordCountJob.getOutputList();
        Collections.sort(outputList, comparator);
        assertEquals(getTuple3("2016030112","#dcflinkmeetup",2),outputList.get(0));
        assertEquals(getTuple3("2016030112","#flink",1),outputList.get(1));
        assertEquals(getTuple3("2016030113","#dcflinkmeetup",1),outputList.get(2));
        assertEquals(getTuple3("2016030113","#flink",1),outputList.get(3));        
    }

    @Test
    public void testExecuteJobWithInputFile() {
        File inputFile = new File(SAMPLE_INPUT_PATH, SAMPLE_INPUT_FILE_NAME);
        String[] args = {"--input",inputFile.getAbsolutePath()};
        BatchWordCount wordCountJob = new BatchWordCount(args);
        wordCountJob.initializeExecutionEnvironment(ExecutionEnvironment.createLocalEnvironment(1));
        wordCountJob.executeJob();
        List<Tuple3<String, String, Integer>> outputList = wordCountJob.getOutputList();
        Collections.sort(outputList, comparator);
        assertEquals(getTuple3("2016030112","#dcflinkmeetup",2),outputList.get(0));
        assertEquals(getTuple3("2016030112","#flink",1),outputList.get(1));
        assertEquals(getTuple3("2016030113","#dcflinkmeetup",1),outputList.get(2));
        assertEquals(getTuple3("2016030113","#flink",1),outputList.get(3));
        
    }

    @Test
    public void testExecuteJobWithInputAndOutputFile() throws IOException {
        
        File inputFile = new File(SAMPLE_INPUT_PATH, SAMPLE_INPUT_FILE_NAME);
        File outputFile = new File(SAMPLE_OUTPUT_PATH);
        String[] args = {"--input",inputFile.getAbsolutePath(), "--output",outputFile.getAbsolutePath()};
        BatchWordCount wordCountJob = new BatchWordCount(args);
        wordCountJob.initializeExecutionEnvironment(ExecutionEnvironment.createLocalEnvironment(1));
        wordCountJob.executeJob();        
        List<String> lines=FileUtils.readLines(outputFile);
        List<Tuple3<String,String,Integer>> outputList = new ArrayList<>();
        for(String line:lines){
            String[] tokens = StringUtils.split(line,',');
            outputList.add(this.getTuple3(tokens[0], tokens[1], Integer.parseInt(tokens[2])));
        }
        Collections.sort(outputList, comparator);
        assertEquals(getTuple3("2016030112","#dcflinkmeetup",2),outputList.get(0));
        assertEquals(getTuple3("2016030112","#flink",1),outputList.get(1));
        assertEquals(getTuple3("2016030113","#dcflinkmeetup",1),outputList.get(2));
        assertEquals(getTuple3("2016030113","#flink",1),outputList.get(3));
    }

    private Tuple3<String,String,Integer> getTuple3(String dtTime,String hashtag,Integer count){
        Tuple3<String,String,Integer> tuple3 = new Tuple3<>();
        tuple3.f0 = dtTime;
        tuple3.f1 = hashtag;
        tuple3.f2 = count;
        return tuple3;
    }

    private Comparator<Tuple3<String, String, Integer>> comparator = (tupleA, tupleB) -> {
        int cmp = tupleA.f0.compareTo(tupleB.f0);
        if (cmp != 0)
            return cmp;
        cmp = tupleA.f1.compareTo(tupleB.f1);
        if (cmp != 0)
            return cmp;
        cmp = tupleA.f2.compareTo(tupleB.f2);
        return cmp;
    };
}
