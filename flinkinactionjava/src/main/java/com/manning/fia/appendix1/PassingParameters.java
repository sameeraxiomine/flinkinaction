package com.manning.fia.appendix1;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class PassingParameters {
    public static boolean RUN_LOCALLY = true;
    public static String HOST = "localhost";
    public static int PORT = 6123;
    public static String JAR_PATH = "target/flinkinactionjava-0.0.1-SNAPSHOT.jar";
    public static int DEFAULT_LOCAL_PARALLELISM = 1;
    public static int DEFAULT_REMOTE_PARALLELISM = 5;

    public static String[] input = { "this is a test", "this is a test",
            "this is a test", "this is a test" };

    public static ExecutionEnvironment getEnvironment(boolean isLocal) {
        ExecutionEnvironment execEnv = null;
        if (isLocal) {
            execEnv = ExecutionEnvironment
                    .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        } else {
            execEnv = ExecutionEnvironment.createRemoteEnvironment(HOST, PORT,
                    JAR_PATH);
            execEnv.setParallelism(DEFAULT_REMOTE_PARALLELISM);
        }
        return execEnv;
    }

    public static void passingParameterUsingConstructor() throws Exception {

        ExecutionEnvironment execEnv = getEnvironment(false);
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(input));
        DataSet<Tuple2<String, Integer>> output = source
                .flatMap(new Tokenizer("TEST1")).groupBy(0).sum(1);
        output.print();

    }

    public static void passingNonSerializableParameterUsingConstructor()
            throws Exception {

        ExecutionEnvironment execEnv = getEnvironment(false);
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(input));
        MyPrefix myPrefix = new MyPrefix();
        myPrefix.prefix = "TEST2";
        DataSet<Tuple2<String, Integer>> output = source
                .flatMap(new Tokenizer2(myPrefix)).groupBy(0).sum(1);
        output.print();

    }

    public static void passingNonSerializableParameterUsingConstructor2()
            throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(false);
        DataSet<String> source = execEnv.fromCollection(Arrays.asList(input));
        MyPrefix2 myPrefix = new MyPrefix2("TEST3");        
        DataSet<Tuple2<String, Integer>> output = source
                .flatMap(new Tokenizer3(myPrefix)).groupBy(0).sum(1);
        output.print();
    }

    public static void main(String[] args) throws Exception {
        // PassingParameters.passingParameterUsingConstructor();
        //PassingParameters.passingNonSerializableParameterUsingConstructor();
        PassingParameters.passingNonSerializableParameterUsingConstructor2();
    }

    public static final class Tokenizer implements
            FlatMapFunction<String, Tuple2<String, Integer>> {
        private String prefix;

        public Tokenizer(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split(" ");
            for (String token : tokens) {
                out.collect(new Tuple2<>(prefix + "_" + token, 1));
            }
        }
    }

    public static final class Tokenizer2 implements
            FlatMapFunction<String, Tuple2<String, Integer>> {
        private String prefix;

        public Tokenizer2(MyPrefix prefix) {
            this.prefix = prefix.prefix;
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split(" ");
            for (String token : tokens) {
                out.collect(new Tuple2<>(prefix + "_" + token, 1));
            }
        }
    }

    public static final class Tokenizer3 implements
            FlatMapFunction<String, Tuple2<String, Integer>> {
        private String prefix;

        public Tokenizer3(MyPrefix2 prefix) {
            this.prefix = prefix.getPrefix();
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split(" ");
            for (String token : tokens) {
                out.collect(new Tuple2<>(prefix + "_" + token, 1));
            }
        }
    }

    public static final class MyPrefix {
        public String prefix;

    }
    
    public static final class MyPrefix2 {
        private String prefix;

        public MyPrefix2(String prefix) {
            super();
            this.prefix = prefix;
        }

        public String getPrefix() {
            return prefix;
        }
    }

}
