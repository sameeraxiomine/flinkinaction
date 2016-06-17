package com.manning.fia.c04;

import com.manning.fia.transformations.media.MapTokenizeNewsFeed;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TumblingWindowCountForMedia {

    public void executeJob() {
        try {
            final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
            final DataStream<String> socketStream = execEnv.socketTextStream("localhost", 9000);
            socketStream.map(new MapTokenizeNewsFeed())
                    .keyBy(0, 1)
                    .countWindow(3)
                    .sum(2)
                    .print();
            execEnv.execute("Tumbling Count Window");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        final TumblingWindowCountForMedia window = new TumblingWindowCountForMedia();
        window.executeJob();

    }
}
