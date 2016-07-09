package com.manning.fia.c02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Flink Streaming WordCount Example
 */
public class SimpleStreamingWordCount {

 
  public static void main(String[] args) throws Exception {
    //#1. Fetch StreamExecutionEnvironment
      StreamExecutionEnvironment execEnv =
             StreamExecutionEnvironment.createLocalEnvironment(1); 
      //execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

      String[] lines = { "201603011201,#DCFlinkMeetup",
              "201603011201,#DcFlinkMeetup", "201603011201,#Flink",
              "201603011201,#Flink", "201603011201,#DCFlinkMeetup" };

      //#2. Create sample data source from in-memory collection
      DataStream<String> source = execEnv.fromCollection(Arrays.asList(lines)); 

      DataStream<Tuple3<String, String, Integer>> counts = 
      //#3. Tokenize each line
      
      source.map(new Tokenizer()) 
      //#4. Key By the 0th and 1st attribute of tokenized line
            .keyBy(0, 1) 
      //#5. Aggregate the 2nd attribute to obtain word count
           .sum(2); 
      //#6. Collect results (or write to sink)
      /*
      Iterator<Tuple3<String,String,Integer>> iter = 
        DataStreamUtils.collect(counts);
      List<Tuple3<String,String,Integer>> output = Lists.newArrayList(iter);
      //#7. Display the output. Typically get from a distributed file.
      for (Tuple3<String,String,Integer> line : output) {
         System.err.println(line.f0 +","+line.f1 + ","+line.f2);
      }
      */
      counts.printToErr();
      execEnv.execute();
      
      //#7.1 Alternative - Send output to console
      //counts.print();     
      //#7.2 Alternative - Send output to a CSV file where you specify the line and field separators
      //counts.writeAsCsv(filePath, "\n", ",");
      //#8 Call StreamExecutionEnvironment.execute() to trigger execution
      //execEnv.execute();
      

  }
  



  public static final class Tokenizer implements
      MapFunction<String, Tuple3<String, String, Integer>> {

    @Override
    public Tuple3<String, String, Integer> map(String value) throws Exception {
            String[] tokens = value.toLowerCase().split(",");
            String newDt = tokens[0].substring(0, 10);
            String word = tokens[1].toLowerCase();            
            return new Tuple3<>(newDt, word, 1);
          }
    }
  
  
 

}

