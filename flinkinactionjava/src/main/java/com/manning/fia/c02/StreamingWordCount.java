package com.manning.fia.c02;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.manning.fia.utils.datagen.HashTagGenerator;
import com.manning.fia.utils.datagen.IDataGenerator;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink Streaming WordCount Example
 */
public class StreamingWordCount {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingWordCount.class);

  private final ParameterTool params;
  private StreamExecutionEnvironment execEnv;

  private List<Tuple3<String, String, Integer>> outputList;
  private IDataGenerator<String> dataGenerator;
  private boolean printToConsole = false;

  public StreamingWordCount(String[] args) {
    params = ParameterTool.fromArgs(args);
  }

  public void setDataGenerator(IDataGenerator<String> generator) {
    this.dataGenerator = generator;
  }

  public void printToConsole() {
    this.printToConsole = true;
  }

  public void initializeExecutionEnvironment(StreamExecutionEnvironment execEnv) {
    this.execEnv = execEnv;
  }

  public void initializeEnvironment() {
    // set up the execution environment
    this.execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    // make parameters available in the web interface
    this.execEnv.getConfig().setGlobalJobParameters(params);
  }

  public StreamExecutionEnvironment getStreamExecutionEnvironment() { return this.execEnv; }

  public void executeJob() {
    try {
      DataStream<String> inputDataStream;
      if (params.has("input")) {
        LOG.info("Reading the file from --input parameter");
        inputDataStream = this.execEnv.readTextFile(params.get("input"));
      } else {
        LOG.info("Execute job with generated data");
        LOG.info("Alternatively use --input to specify input file");
        inputDataStream = this.execEnv.fromCollection(this.dataGenerator.getData());
      }

      DataStream<Tuple3<String, String, Integer>> counts =
          // split up the lines in pairs (3-tuples) containing: (date-time,word,1)
          inputDataStream.map(new Tokenizer())
              // group by the tuple field "0","1" and sum up tuple field "2"
              .keyBy(0, 1)
              .sum(2);

      // emit result
      if (params.has("output")) {
        LOG.info("Writing to file from --output parameter");
        counts.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE, "\n", ",");
        execEnv.execute("Streaming WordCount Example");

      } else {
        LOG.info("No --output parameter specified. Collecting to list or to stdout");
        if (this.printToConsole) {
          counts.print();
        } else {
          Iterator<Tuple3<String, String, Integer>> iter = DataStreamUtils.collect(counts);
          outputList = Lists.newArrayList(iter);
        }
      }
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }
  }

  public List<Tuple3<String, String, Integer>> getOutputList() {
    return this.outputList;
  }

  public static void main(String[] args) throws Exception {
    //#1. Fetch StreamExecutionEnvironment
      StreamExecutionEnvironment execEnv =
             StreamExecutionEnvironment.getExecutionEnvironment(); 
      execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

      String[] lines = { "201603011201,#DCFlinkMeetup",
                         "201603011202,#DcFlinkMeetup",
                         "201603011203,#Flink", 
                         "201603011302,#Flink", 
                         "201603011302,#DCFlinkMeetup" };
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
      Iterator<Tuple3<String,String,Integer>> iter = 
        DataStreamUtils.collect(counts);
      List<Tuple3<String,String,Integer>> output = Lists.newArrayList(iter);
      //#7. Display the output. Typically get from a distributed file.
      for (Tuple3<String,String,Integer> line : output) {
         System.err.println(line.f0 +","+line.f1 + ","+line.f2);
      }
      //#7.1 Alternative - Send output to console
      //counts.print();     
      //#7.2 Alternative - Send output to a CSV file where you specify the line and field separators
      //counts.writeAsCsv(filePath, "\n", ",");
      //#8 Call StreamExecutionEnvironment.execute() to trigger execution
      execEnv.execute();


  }
  


  /**
   * Implements the string tokenizer that splits sentences into words as a
   * user-defined FlatMapFunction. The function takes a line (String) and
   * splits it into multiple pairs in the form of "(word,1)" (
   * {@code Tuple2<String, Integer>}).
   */
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

