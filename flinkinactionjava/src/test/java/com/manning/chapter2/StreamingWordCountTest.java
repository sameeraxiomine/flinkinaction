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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.manning.fia.c02.StreamingWordCount;
import com.manning.fia.utils.datagen.HashTagGenerator;
import com.manning.fia.utils.datagen.IDataGenerator;

public class StreamingWordCountTest {
  public static String[] lines = { "20160301120100,#DCFlinkMeetup",
      "20160301120200,#DcFlinkMeetup", "20160301120300,#Flink",
      "20160301130200,#Flink", "20160301130200,#DCFlinkMeetup" };

  private IDataGenerator<String> dataGenerator = null;

  @Before
  public  void setup() throws IOException {
    // Delete Input/Output Folder if exists
    this.cleanup();
    this.dataGenerator = new HashTagGenerator();
    dataGenerator.setData(Arrays.asList(lines));
  }

  @After
  public  void cleanup() throws IOException {
  }

  @Test
  public void testExecuteJobWithTestData() throws Exception {
    StreamingWordCount wordCountJob = new StreamingWordCount(new String[0]);
    wordCountJob.setDataGenerator(this.dataGenerator);
    wordCountJob.initializeExecutionEnvironment(StreamExecutionEnvironment.createLocalEnvironment(1));
    wordCountJob.executeJob();
    List<Tuple3<String, String, Integer>> outputList = wordCountJob.getOutputList();
    Collections.sort(outputList, comparator);
    assertEquals(getTuple3("2016030112","#dcflinkmeetup", 1),outputList.get(0));
    assertEquals(getTuple3("2016030112","#dcflinkmeetup", 2),outputList.get(1));
    assertEquals(getTuple3("2016030112","#flink", 1),outputList.get(2));
    assertEquals(getTuple3("2016030113","#dcflinkmeetup", 1),outputList.get(3));
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
    cmp = tupleA.f0.compareTo(tupleB.f0);
    return cmp;
  };
}
