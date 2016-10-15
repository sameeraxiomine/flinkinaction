package com.manning.fia.c07;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;
import java.util.Random;

public class CEPExample {



    public void executeJob(ParameterTool parameterTool) throws Exception {
        final DataStream<EngineCarModule> stream;
        final StreamExecutionEnvironment execEnv;
        final int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        stream = execEnv.addSource(new ECMSourceFunction());


        Pattern<EngineCarModule, ?> pattern =
                Pattern.<EngineCarModule>begin("start")
                        .subtype(SpeedSensor.class)
                        .where(new FilterFunction<SpeedSensor>() {
                            @Override
                            public boolean filter(SpeedSensor speedSensor) throws Exception {
                                return speedSensor.getReading() > 65;
                            }
                        })
                        .next("next")
                        .subtype(BrakeSensor.class)
                        .where(new FilterFunction<BrakeSensor>() {
                            @Override
                            public boolean filter(BrakeSensor brakeSensor) throws Exception {
                                return brakeSensor.getReading() > 5;
                            }
                        })
                        .followedBy("end")
                        .subtype(SpeedSensor.class)
                        .where(new FilterFunction<SpeedSensor>() {
                            @Override
                            public boolean filter(SpeedSensor speedSensor) throws Exception {
                                return speedSensor.getReading() < 60;
                            }
                        }).within(Time.milliseconds(10));


        DataStream<String> result = CEP.pattern(stream, pattern)
                .select(
                        new PatternSelectFunction<EngineCarModule, String>() {
                            @Override
                            public String select(Map<String, EngineCarModule> pattern) throws Exception {
                                StringBuilder builder = new StringBuilder();
                                builder.append(pattern.get("start").getReading()).append("-----------")
                                        .append(pattern.get("next").getReading()).append("------------")
                                        .append(pattern.get("end").getReading());

                                return builder.toString();
                            }
                        });
        result.printToErr();
        execEnv.execute();
    }


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        CEPExample window = new CEPExample();
        window.executeJob(parameterTool);

    }


}
