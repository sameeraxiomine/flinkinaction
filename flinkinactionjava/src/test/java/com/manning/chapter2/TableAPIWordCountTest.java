package com.manning.chapter2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import com.manning.chapter2.TableAPIWordCount.Word;
import com.manning.utils.datagen.HashTagGenerator;
import com.manning.utils.datagen.IDataGenerator;

public class TableAPIWordCountTest {
    public static String SAMPLE_INPUT_FILE_NAME = "hashtags.txt";
    public static String SAMPLE_INPUT_PATH = "src/test/resources/input/";
    public static String SAMPLE_OUTPUT_PATH = "src/test/resources/output/counts";
    public static String[] lines = { "201603011201,#DCFlinkMeetup",
            "201603011202,#DcFlinkMeetup", "201603011203,#Flink",
            "201603011302,#Flink", "201603011302,#DCFlinkMeetup" };

    private IDataGenerator<String> dataGenerator = null;

    @Before
    public  void setup() throws IOException {
        this.dataGenerator = new HashTagGenerator();
        dataGenerator.setData(Arrays.asList(lines));
        // Delete Input Folder if exists
        this.cleanup();
        FileUtils.writeLines(
                new File(SAMPLE_INPUT_PATH, SAMPLE_INPUT_FILE_NAME),
                Arrays.asList(lines));
    }

    
    public  void cleanup() throws IOException {
        FileUtils.deleteQuietly(new File(SAMPLE_INPUT_PATH));
        FileUtils.deleteQuietly(new File(SAMPLE_OUTPUT_PATH));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testExecuteJobWithTestData() {
        TableAPIWordCount batchWordCount = new TableAPIWordCount(new String[0]);
        batchWordCount.setDateGenerator(dataGenerator);
        //batchWordCount.printToConsole();
        batchWordCount.executeJob();
        
        List<TableAPIWordCount.Word> outputList = batchWordCount
                .getOutputList();
        Collections.sort(outputList, comparator);
        assertEquals(getWord("2016030112","#dcflinkmeetup",2),outputList.get(0));
        assertEquals(getWord("2016030112","#flink",1),outputList.get(1));
        assertEquals(getWord("2016030113","#dcflinkmeetup",1),outputList.get(2));
        assertEquals(getWord("2016030113","#flink",1),outputList.get(3));        
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testExecuteJobWithInputFile() {
        File inputFile = new File(SAMPLE_INPUT_PATH, SAMPLE_INPUT_FILE_NAME);
        String[] args = {"--input",inputFile.getAbsolutePath()};
        TableAPIWordCount wordCountJob = new TableAPIWordCount(args);
        //batchWordCount.setDateGenerator(dataGenerator);
        //batchWordCount.printToConsole();
        wordCountJob.executeJob();
        
        List<TableAPIWordCount.Word> outputList = wordCountJob
                .getOutputList();
        Collections.sort(outputList, comparator);
        assertEquals(getWord("2016030112","#dcflinkmeetup",2),outputList.get(0));
        assertEquals(getWord("2016030112","#flink",1),outputList.get(1));
        assertEquals(getWord("2016030113","#dcflinkmeetup",1),outputList.get(2));
        assertEquals(getWord("2016030113","#flink",1),outputList.get(3));
        
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testExecuteJobWithInputAndOutputFile() throws IOException {
        File inputFile = new File(SAMPLE_INPUT_PATH, SAMPLE_INPUT_FILE_NAME);
        File outputFile = new File(SAMPLE_OUTPUT_PATH);
        String[] args = {"--input",inputFile.getAbsolutePath(), "--output",outputFile.getAbsolutePath()};
        TableAPIWordCount wordCountJob = new TableAPIWordCount(args);
        //batchWordCount.setDateGenerator(dataGenerator);
        //batchWordCount.printToConsole();
        wordCountJob.executeJob(1);
        List<String> lines=FileUtils.readLines(outputFile);
        List<Word> outputList = new ArrayList<>();
        for(String line:lines){
            String[] tokens = StringUtils.split(line,',');
            Word word = new Word(tokens[0], tokens[1], Integer.parseInt(tokens[2]));
            outputList.add(word);
        }
        Collections.sort(outputList, comparator);
        assertEquals(getWord("2016030112","#dcflinkmeetup",2),outputList.get(0));
        assertEquals(getWord("2016030112","#flink",1),outputList.get(1));
        assertEquals(getWord("2016030113","#dcflinkmeetup",1),outputList.get(2));
        assertEquals(getWord("2016030113","#flink",1),outputList.get(3));
    }

    private Word getWord(String dtTime,String hashtag,int count){
        return new Word(dtTime,hashtag,count);
    }

    private Comparator<TableAPIWordCount.Word> comparator = (wordA, wordB) -> {
        int cmp = wordA.datetime.compareTo(wordB.datetime);
        if (cmp != 0)
            return cmp;
        cmp = wordA.word.compareTo(wordB.word);
        if (cmp != 0)
            return cmp;
        return 0;
    };
}
