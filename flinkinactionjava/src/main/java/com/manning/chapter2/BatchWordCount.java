package com.manning.chapter2;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import com.manning.utils.datagen.HashTagGenerator;
import com.manning.utils.datagen.IDataGenerator;

@SuppressWarnings("serial")
public class BatchWordCount {
    private final ParameterTool params;
    private final ExecutionEnvironment env;
    
    private static Logger LOG = Logger.getLogger(BatchWordCount.class.getName());
    private List<Tuple3<String,String,Integer>> outputList;
    private IDataGenerator<String> dataGenerator;
    private boolean printToConsole = false;
    public BatchWordCount(String[] args) {
        params = ParameterTool.fromArgs(args);
        // set up the execution environment
        env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
    }
    public void setDateGenerator(IDataGenerator<String> generator){
        this.dataGenerator = generator;
    }

    public void printToConsole() {
        this.printToConsole = true;
    }
    public void executeJob(int sinkParallelism) {        
        try{
            DataSet<String> inputDataSet;
            if (params.has("input")) {
                LOG.info("Reading the file from --input parameter");
                inputDataSet = env.readTextFile(params.get("input"));
            } else {
                LOG.info("Execute job with generated data");
                LOG.info("Alternatively use --input to specify input file");
                inputDataSet = env.fromCollection(this.dataGenerator.getData());
            }
            DataSet<Tuple3<String, String, Integer>> counts =
            // split up the lines in pairs (3-tuples) containing: (date-time,word,1)
            inputDataSet.flatMap(new Tokenizer())
            // group by the tuple field "0","1" and sum up tuple field "2"
                    .groupBy(0, 1).sum(2);
            // emit result
            if (params.has("output")) {
                LOG.info("Writing to file from --output parameter");
                if(sinkParallelism>0){
                    counts.writeAsCsv(params.get("output"), "\n", ",").setParallelism(sinkParallelism);    
                }else{
                    counts.writeAsCsv(params.get("output"), "\n", ",");
                }
                
                // execute program
                env.execute("WordCount Example");
            } else {
                LOG.info("No --output parameter specified. Collecting to list or to stdout");
                if(this.printToConsole){
                    counts.print();
                }else{
                    outputList = counts.collect();    
                }
            }
        }catch(Exception ex){
            Throwables.propagate(ex);
        }


    }

    public void executeJob() { 
        this.executeJob(0);
    }
    public List<Tuple3<String,String,Integer>> getOutputList(){
        return this.outputList;
    }
    
    public static void main(String[] args) throws Exception {        
        BatchWordCount batchWordCount = new BatchWordCount(args);
        batchWordCount.printToConsole();
        IDataGenerator<String> dataGenerator = new HashTagGenerator("030162016", 100L);
        dataGenerator.generateData();
        
        batchWordCount.setDateGenerator(dataGenerator);        
        batchWordCount.executeJob();
        
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" (
     * {@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer implements
            FlatMapFunction<String, Tuple3<String, String, Integer>> {

        @Override
        public void flatMap(String value,
                Collector<Tuple3<String, String, Integer>> out) {
            SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmm");
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
