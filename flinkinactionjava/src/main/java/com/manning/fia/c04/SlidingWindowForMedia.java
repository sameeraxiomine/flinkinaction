package com.manning.fia.c04;

import com.manning.fia.transformations.media.MapTokenizeNewsFeed;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SlidingWindowForMedia {


    public void executeJob() {
        try {
            final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
            final DataStream<String> socketStream = execEnv.socketTextStream("localhost", 9000);
            socketStream.map(new MapTokenizeNewsFeed())
                    .keyBy(0, 1)
                    .timeWindow(Time.seconds(15), Time.seconds(5))
                    .sum(3)
                    .print();

            execEnv.execute("Sliding Windows");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        final SlidingWindowForMedia window = new SlidingWindowForMedia();
        window.executeJob();

    }
}
