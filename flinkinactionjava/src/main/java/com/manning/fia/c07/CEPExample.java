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
        DataStream<SensorEvent> stream;
        final StreamExecutionEnvironment execEnv;
        final int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        stream = execEnv.addSource(new VehicleTelematicsSourceFunction());


        Pattern<SensorEvent, ?> pattern =
                Pattern.<SensorEvent>begin("start")
                        .subtype(SpeedSensorEvent.class)
                        .where(new FilterFunction<SpeedSensorEvent>() {
                            @Override
                            public boolean filter(SpeedSensorEvent speedSensor) throws Exception {
                                return speedSensor.reading > 65;
                            }
                        })
                        .next("next")
                        .subtype(BrakeSensorEvent.class)
                        .where(new FilterFunction<BrakeSensorEvent>() {
                            @Override
                            public boolean filter(BrakeSensorEvent brakeSensor) throws Exception {
                                return brakeSensor.reading > 5;
                            }
                        })
                        .followedBy("end")
                        .subtype(SpeedSensorEvent.class)
                        .where(new FilterFunction<SpeedSensorEvent>() {
                            @Override
                            public boolean filter(SpeedSensorEvent speedSensor) throws Exception {
                                return speedSensor.reading < 60;
                            }
                        }).within(Time.milliseconds(10));


        
        final DataStream<SensorEvent> sensorStream;
         //keyby deviceId
        sensorStream=stream.keyBy(new KeySelector<SensorEvent, Integer>() {
            @Override
            public Integer getKey(SensorEvent SensorEvent) throws Exception {
                return SensorEvent.getDeviceId();
            }
        });
        
        //create SensorStream
        /*
        sensorStream = stream.map(new MapFunction<SensorEvent, SensorEvent>() {
            @Override
            public SensorEvent map(SensorEvent sensorEvent) throws Exception {
                return sensorEvent;
            }
        });
*/
        DataStream<Alert> result = CEP.pattern(sensorStream, pattern)
                .select(
                        new PatternSelectFunction<SensorEvent, Alert>() {
                            @Override
                            public Alert select(Map<String, SensorEvent> pattern) throws Exception {
                           	  return new Alert(pattern.get("start"),pattern.get("next"),pattern.get("end"));
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
