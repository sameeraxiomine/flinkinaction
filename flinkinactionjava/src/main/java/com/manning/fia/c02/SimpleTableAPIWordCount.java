package com.manning.fia.c02;

import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.table.Table;
import org.apache.flink.shaded.com.google.common.base.Throwables;

import com.manning.fia.utils.datagen.HashTagGenerator;
import com.manning.fia.utils.datagen.IDataGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class SimpleTableAPIWordCount {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleTableAPIWordCount.class);

    private final ParameterTool params;
    private ExecutionEnvironment execEnv;
    private TableEnvironment tblEnv;

    private List<Word> outputList;
    private IDataGenerator<String> dataGenerator;
    private boolean printToConsole = false;

    public SimpleTableAPIWordCount(String[] args) {
        params = ParameterTool.fromArgs(args);

    }
    public void initializeExecutionEnvironment(ExecutionEnvironment execEnv){
        this.execEnv = execEnv;        
        tblEnv = new TableEnvironment();
        
    }
    public void initializeExecutionEnvironment(){
        // set up the execution environment
        execEnv = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        execEnv.getConfig().setGlobalJobParameters(params);
        tblEnv = new TableEnvironment();
    }

    public void setDateGenerator(IDataGenerator<String> generator) {
        this.dataGenerator = generator;
    }

    public void printToConsole() {
        this.printToConsole = true;
    }

    public void executeJob() {
        try {
            DataSet<String> inputDataSet;
            if (params.has("input")) {
                LOG.info("Reading the file from --input parameter");
                
                inputDataSet = execEnv.readTextFile(params.get("input"));
            } else {
                LOG.info("Execute job with generated data");
                LOG.info("Alternatively use --input to specify input file");
                inputDataSet = execEnv.fromCollection(this.dataGenerator.getData());
            }
            DataSet<Tuple3<String, String, Integer>> intermediateDS =
            // split up the lines in pairs (3-tuples) containing:
            // (date-time,word,1)
            inputDataSet.flatMap(new BatchWordCount.Tokenizer());
            Table table = tblEnv.fromDataSet(intermediateDS,
                    "datetime,word,index");
            Table output = table.groupBy("datetime,word").select(
                    "datetime,word, word.count as wrdCnt");
            DataSet<Word> result = tblEnv.toDataSet(output, Word.class);
            // emit result
            if (params.has("output")) {
                LOG.info("Writing to file from --output parameter");
                result.writeAsText(params.get("output"));                // execute program
                execEnv.execute("TableAPI WordCount Example");
            } else {
                LOG.info("No --output parameter specified. Collecting to list or to stdout");
                if (this.printToConsole) {
                    result.print();
                } else {
                    this.outputList = result.collect();
                }
            }
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }

    }



    public List<Word> getOutputList() {
        return this.outputList;
    }

    public static void main(String[] args) throws Exception {
        SimpleTableAPIWordCount wordCountJob = new SimpleTableAPIWordCount(args);
        wordCountJob.initializeExecutionEnvironment(ExecutionEnvironment.createLocalEnvironment());
        wordCountJob.printToConsole();
        IDataGenerator<String> dataGenerator = new HashTagGenerator(
                "030162016", 100L);
        dataGenerator.generateData();

        wordCountJob.setDateGenerator(dataGenerator);
        
        wordCountJob.executeJob();

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
    /*
     * public static final class Tokenizer implements FlatMapFunction<HashTag,
     * HashTag> {
     * 
     * @Override public void flatMap(HashTag value, Collector<HashTag> out) {
     * SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmm");
     * SimpleDateFormat ouputFormat = new SimpleDateFormat("yyyyMMddHH"); try {
     * Date inputDt = inputFormat.parse(value.dtTime); String outputDt =
     * ouputFormat.format(inputDt); value.dtTime=outputDt; out.collect(value); }
     * catch (Exception ex) { Throwables.propagate(ex); } } }
     */
    public static class Word implements Comparator<Word> {
        public Word(String datetime, String word, int wrdCnt) {
            this.datetime = datetime;
            this.word = word;
            this.wrdCnt = wrdCnt;
        }

        public Word() {
        } // empty constructor to satisfy POJO requirements

        public String datetime;
        public String word;
        public int wrdCnt;

        @Override
        public String toString() {
            return datetime+","+word+","+wrdCnt;
        }

        @Override
        public int compare(Word wordA, Word wordB) {
            int cmp = wordA.datetime.compareTo(wordB.datetime);
            if (cmp != 0)
                return cmp;
            cmp = wordA.word.compareTo(wordB.word);
            if (cmp != 0)
                return cmp;
            return 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((datetime == null) ? 0 : datetime.hashCode());
            result = prime * result + ((word == null) ? 0 : word.hashCode());
            result = prime * result + wrdCnt;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Word other = (Word) obj;
            if (datetime == null) {
                if (other.datetime != null)
                    return false;
            } else if (!datetime.equals(other.datetime))
                return false;
            if (word == null) {
                if (other.word != null)
                    return false;
            } else if (!word.equals(other.word))
                return false;
            return wrdCnt == other.wrdCnt;
        }
        
        
    }

}
