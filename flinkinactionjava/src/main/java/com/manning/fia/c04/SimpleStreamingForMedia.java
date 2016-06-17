package com.manning.fia.c04;

import com.manning.fia.transformations.media.MapTokenizeNewsFeed;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * this example basically does a simple streaming
 * i.e grouping the data keys and doing a sum aggregation  cummulatively as & when the data arrives.
 * no concept of Windows as it is basically KeyedStream
 *
 */
public class SimpleStreamingForMedia {


    public void executeJob() {
        try {
            final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
            final DataStream<String> socketStream = execEnv.socketTextStream("localhost", 9000);
            socketStream.map(new MapTokenizeNewsFeed())
                    .keyBy(0, 1)
                    .sum(3)
                    .print();

            execEnv.execute("Simple Streaming");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        final SimpleStreamingForMedia window = new SimpleStreamingForMedia();
        window.executeJob();

    }
}
