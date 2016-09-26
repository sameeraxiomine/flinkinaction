package com.manning.fia.newc06;

import com.manning.fia.utils.DataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.joda.time.format.DateTimeFormat;

/**

 */


public class JoinedStreamsExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        DataStream<String> temparatureDataStream;
        DataStream<String> pressureDataStream;
        DataStream<Tuple3<String, Double, String>> temparatureStream;
        DataStream<Tuple3<String, Double, String>> pressureStream;

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        final int parallelism = parameterTool.getInt("parallelism", 1);

        execEnv.setParallelism(parallelism);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        temparatureDataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));
        pressureDataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

        temparatureStream = temparatureDataStream.map(new SensorMapper());
        pressureStream = pressureDataStream.map(new SensorMapper());


        temparatureStream = temparatureStream.
                assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

        pressureStream = pressureStream.
                assignTimestampsAndWatermarks(new TimestampAndWatermarkAssigner());

        temparatureStream.join(pressureStream).where(temparatureStream.keyBy(0).getKeySelector()).
                equalTo(pressureStream.keyBy(0).getKeySelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(1l)))
                .apply(new SensorJoin());

        execEnv.execute("Joined Streams Example");
    }

    private class SensorMapper implements MapFunction<String, Tuple3<String, Double, String>> {
        @Override
        public Tuple3<String, Double, String> map(String value) throws Exception {
            final String[] tokens = StringUtils.splitPreserveAllTokens(value, "|");
            return new Tuple3<>(tokens[0], Double.valueOf(tokens[1]), tokens[2]);
        }
    }

    private static class SensorJoin implements JoinFunction<Tuple3<String, Double, String>,
            Tuple3<String, Double, String>,
            Tuple3<String, Double, Double>> {

        @Override
        public Tuple3<String, Double, Double> join(Tuple3<String, Double, String> tempatureTuple,
                                                   Tuple3<String, Double, String> pressureTuple) throws
                Exception {
            return new Tuple3<>(
                    tempatureTuple.f0,
                    tempatureTuple.f1,
                    pressureTuple.f1);

        }
    }

    private static class TimestampAndWatermarkAssigner
            implements
            AssignerWithPeriodicWatermarks<Tuple3<String, Double, String>> {
        private static final long serialVersionUID = 1L;
        private long wmTime = 0;
        private long priorWmTime = 0;
        private long lastTimeOfWaterMarking = System.currentTimeMillis();

        @Override
        public Watermark getCurrentWatermark() {
            if (wmTime == priorWmTime) {
                long advance = (System.currentTimeMillis() - lastTimeOfWaterMarking);
                wmTime += advance;// Start advancing
            }
            priorWmTime = wmTime;
            lastTimeOfWaterMarking = System.currentTimeMillis();
            return new Watermark(wmTime);
        }

        @Override
        public long extractTimestamp(
                Tuple3<String, Double, String> element,
                long previousElementTimestamp) {

            long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
                    .parseDateTime(element.f2)
                    .getMillis();
            wmTime = Math.max(wmTime, millis);
            return Long.valueOf(millis);
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        JoinedStreamsExample window = new JoinedStreamsExample();
        window.executeJob(parameterTool);
    }
}




