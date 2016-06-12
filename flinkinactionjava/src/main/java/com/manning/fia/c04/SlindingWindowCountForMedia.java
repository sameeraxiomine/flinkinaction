package com.manning.fia.c04;

import com.manning.fia.transformations.media.MapTokenizeNewsFeed;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by hari on 5/30/16.
 */
public class SlindingWindowCountForMedia {

    public void executeJob() {
        try {
            final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
            final DataStream<String> socketStream = execEnv.socketTextStream("localhost", 9000);
//            socketStream.map(new MapTokenizeNewsFeed())
//                    .keyBy(0, 1)
//                    .countWindow(4,1)
//                    .sum(2)
//                    .print();
            execEnv.execute("Sliding Count Window");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        final SlindingWindowCountForMedia window = new SlindingWindowCountForMedia();
        window.executeJob();

    }
}

