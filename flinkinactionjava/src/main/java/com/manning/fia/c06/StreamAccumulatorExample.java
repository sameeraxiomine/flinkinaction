package com.manning.fia.c06;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamAccumulatorExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {


        DataStream<Tuple2<String, Integer>> stream;
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        final int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv.setParallelism(parallelism);

        stream = execEnv.addSource(new ContinousEventsSource());
        stream.flatMap(new RulesMapFunction(45, 450));
        execEnv.execute();
    }


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamAccumulatorExample streamAccumulatorExample = new StreamAccumulatorExample();
        streamAccumulatorExample.executeJob(parameterTool);

    }

    public class RulesMapFunction extends
            RichFlatMapFunction<Tuple2<String, Integer>,
                    Tuple3<String, Integer, String>> {

        private Integer temperatureThreshold = null;
        private Integer pressureThreshold = null;

        private IntCounter numberofEventsCounter = new IntCounter();
        private IntCounter numberofAlarmCounter = new IntCounter();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("numberofEventsCounter", numberofEventsCounter);
            getRuntimeContext().addAccumulator("numberofAlarmCounter", numberofAlarmCounter);
        }

        public RulesMapFunction(int temperatureThreshold, int pressureThreshold) {
            this.temperatureThreshold = temperatureThreshold;
            this.pressureThreshold = pressureThreshold;
        }


        @Override
        public void flatMap(Tuple2<String, Integer> event, Collector<Tuple3<String, Integer, String>> out)
                throws Exception {
            numberofEventsCounter.add(1);
            int threshold = event.f0.equals("temperature") ? temperatureThreshold : pressureThreshold;
            if (event.f1 < threshold) {
                numberofAlarmCounter.add(1);
                out.collect(Tuple3.of(event.f0, event.f1, "ALERT"));
            } else {
                out.collect(Tuple3.of(event.f0, event.f1, "NORMAL"));
            }
        }


    }
}