package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * this example basically does a simple streaming i.e grouping the data keys and
 * doing a sum aggregation cummulatively as & when the data arrives. no concept
 * of Windows as it is basically KeyedStream
 *
 */
public class SimpleStreamingExample {

    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                    .createLocalEnvironment(1);
        DataStream<String> socketStream = execEnv.socketTextStream(
                    "localhost", 9000);
        socketStream.map(new NewsFeedMapper()).keyBy(1, 2).sum(4).print();
        execEnv.execute("Simple Streaming");
    }

    public static void main(String[] args) throws Exception {
        final SimpleStreamingExample window = new SimpleStreamingExample();
        window.executeJob();
    }
}
