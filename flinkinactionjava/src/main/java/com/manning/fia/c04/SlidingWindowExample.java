package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class SlidingWindowExample {


    public void executeJob(ParameterTool parameterTool) {
        try {
            StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                    .createLocalEnvironment(1);
            execEnv.setParallelism(parameterTool.getInt("parallelism", 1));

            final DataStream<String> dataStream;
            boolean isKafka = parameterTool.getBoolean("isKafka", false);
            if (isKafka) {
                dataStream = execEnv.addSource(NewsFeedDataSource.getKafkaDataSource(parameterTool));
            } else {
                dataStream = execEnv.addSource(NewsFeedDataSource.getCustomDataSource(parameterTool));
            }



            DataStream<Tuple3<String, String, Long>> selectDS = dataStream
                    .map(new NewsFeedMapper()).project(1, 2, 4);

             KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS = selectDS.
                     keyBy(1, 2);

             WindowedStream<Tuple3<String, String, Long>, Tuple, TimeWindow> windowedStream = keyedDS
                     .timeWindow(Time.seconds(15), Time.seconds(5));

             DataStream<Tuple3<String, String, Long>> result = windowedStream.sum(2);

            result.print();

            execEnv.execute("Sliding Windows");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        new NewsFeedSocket().start();
         SlidingWindowExample window = new SlidingWindowExample();
        window.executeJob(parameterTool);

    }
}
