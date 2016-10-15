package com.manning.fia.c07;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Map;

public class CEPExample {


    public void executeJob(ParameterTool parameterTool) throws Exception {
        DataStream<EngineComponentModule> stream;
        final StreamExecutionEnvironment execEnv;
        final int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        stream = execEnv.addSource(new ECMSourceFunction());


        Pattern<Sensor, ?> pattern =
                Pattern.<Sensor>begin("start")
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


        final DataStream<Sensor> sensorStream;

         //keyby deviceId
        stream=stream.keyBy(new KeySelector<EngineComponentModule, Integer>() {
            @Override
            public Integer getKey(EngineComponentModule engineComponentModule) throws Exception {
                return engineComponentModule.getDeviceId();
            }
        });

        //create SensorStream
        sensorStream = stream.map(new MapFunction<EngineComponentModule, Sensor>() {
            @Override
            public Sensor map(EngineComponentModule engineComponentModule) throws Exception {
                return engineComponentModule.getSensor();
            }
        });

        DataStream<String> result = CEP.pattern(sensorStream, pattern)
                .select(
                        new PatternSelectFunction<Sensor, String>() {
                            @Override
                            public String select(Map<String, Sensor> pattern) throws Exception {
                                StringBuilder builder = new StringBuilder();
                                builder.append(pattern.get("start").getType()).append("-----------")
                                        .append(pattern.get("start").getReading()).append("-----------")
                                        .append(pattern.get("next").getType()).append("-----------")
                                        .append(pattern.get("next").getReading()).append("------------")
                                        .append(pattern.get("end").getType()).append("-----------")
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
