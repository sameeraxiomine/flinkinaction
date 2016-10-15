package com.manning.fia.c07;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class InsuranceCEPExample {

    // SpeedLimit & TimeStamp
    private static SpeedSensor[] SPEED_SENSOR = {
            new SpeedSensor(1, 70, 5L),
            new SpeedSensor(1, 72, 10L),
            new SpeedSensor(1, 68, 15L),
            new SpeedSensor(1, 55, 20L),
            new SpeedSensor(1, 50, 25L),
            new SpeedSensor(1, 54, 30L),
            new SpeedSensor(1, 60, 35L),
            new SpeedSensor(1, 65, 40L),
            new SpeedSensor(1, 70, 45L)
    };

    //break range 0-10 and Timestamp
    private static BrakeSensor[] BREAK_SENSOR = {
            new BrakeSensor(1, 0, 5L),
            new BrakeSensor(1, 0, 10L),
            new BrakeSensor(1, 1, 15L),
            new BrakeSensor(1, 8, 20L),
            new BrakeSensor(1, 3, 25L),
            new BrakeSensor(1, 0, 30L),
            new BrakeSensor(1, 0, 35L),
            new BrakeSensor(1, 0, 40L),
            new BrakeSensor(1, 0, 45L)
    };

    public class EventsSource implements SourceFunction<Car> {

        private volatile boolean running = true;
        private Random rnd = new Random();

        public void run(SourceContext<Car> sourceContext) throws Exception {
            int i = 0;
            while (i <= 8) {
                sourceContext.collect(SPEED_SENSOR[i]);
                sourceContext.collect(BREAK_SENSOR[i]);
                i++;
            }
        }

        public void cancel() {
            running = false;
        }
    }


    public void executeJob(ParameterTool parameterTool) throws Exception {
        final DataStream<Car> stream;
        final StreamExecutionEnvironment execEnv;
        final int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv = StreamExecutionEnvironment.createLocalEnvironment(parallelism);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        stream = execEnv.addSource(new EventsSource());

        stream.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Car>() {
            @Override
            public Watermark checkAndGetNextWatermark(Car car, long timeStamp) {
                return new Watermark(timeStamp);
            }

            @Override
            public long extractTimestamp(Car car, long timeStamp) {
                return car.getTimeStamp();
            }
        });


        Pattern<Car, ?> pattern =
                Pattern.<Car>begin("start")
                        .subtype(SpeedSensor.class)
                        .where(new FilterFunction<SpeedSensor>() {
                            @Override
                            public boolean filter(SpeedSensor speedSensor) throws Exception {
                                return speedSensor.getSpeedReading() > 65;
                            }
                        })
                        .next("next")
                        .subtype(BrakeSensor.class)
                        .where(new FilterFunction<BrakeSensor>() {
                            @Override
                            public boolean filter(BrakeSensor brakeSensor) throws Exception {
                                return brakeSensor.getBreakReading() > 5;
                            }
                        })
                        .followedBy("end")
                        .subtype(SpeedSensor.class)
                        .where(new FilterFunction<SpeedSensor>() {
                            @Override
                            public boolean filter(SpeedSensor speedSensor) throws Exception {
                                return speedSensor.getSpeedReading() < 55;
                            }
                        });


        DataStream<String> result = CEP.pattern(stream, pattern)
                .select(
                        new PatternSelectFunction<Car, String>() {
                            @Override
                            public String select(Map<String, Car> pattern) throws Exception {
                                StringBuilder builder = new StringBuilder();
                                builder.append(pattern.get("start")).append(",")
                                        .append(pattern.get("next")).append(",")
                                        .append(pattern.get("end"));

                                return builder.toString();
                            }
                        });
        result.print();
        execEnv.execute();
    }


    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        InsuranceCEPExample window = new InsuranceCEPExample();
        window.executeJob(parameterTool);

    }


}
