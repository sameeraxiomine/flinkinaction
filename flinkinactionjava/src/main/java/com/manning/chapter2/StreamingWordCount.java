package com.manning.chapter2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.manning.utils.datagen.HashTagGenerator;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.DataStreamUtils;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.manning.utils.datagen.IDataGenerator;

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
          inputDataStream.flatMap(new Tokenizer())
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
    IDataGenerator<String> dataGenerator = new HashTagGenerator("030162016", 100L);
    dataGenerator.generateData();
    StreamingWordCount streamingWordCount = new StreamingWordCount(args);
    streamingWordCount.initializeExecutionEnvironment(StreamExecutionEnvironment.createLocalEnvironment(1));
    streamingWordCount.setDataGenerator(dataGenerator);
    streamingWordCount.printToConsole();
    streamingWordCount.executeJob();
    streamingWordCount.execEnv.execute("Running Streaming Word Count");
  }

  /**
   * Implements the string tokenizer that splits sentences into words as a
   * user-defined FlatMapFunction. The function takes a line (String) and
   * splits it into multiple pairs in the form of "(word,1)" (
   * {@code Tuple2<String, Integer>}).
   */
  public static final class Tokenizer implements
      FlatMapFunction<String, Tuple3<String, String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) {
      SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmmss");
      SimpleDateFormat ouputFormat = new SimpleDateFormat("yyyyMMddHH");
      try {
        if (!StringUtils.isBlank(value)) {
          String[] tokens = value.toLowerCase().split(",");
          Date inputDt = inputFormat.parse(tokens[0]);
          String outputDt = ouputFormat.format(inputDt);
          String word = tokens[1].toLowerCase();
          out.collect(new Tuple3<>(outputDt, word, 1));
        }
      } catch (Exception ex) {
        Throwables.propagate(ex);
      }
    }
  }
}

