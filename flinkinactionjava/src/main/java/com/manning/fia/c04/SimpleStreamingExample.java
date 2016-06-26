package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * this example basically does a simple streaming i.e grouping the data keys and
 * doing a sum aggregation cummulatively as & when the data arrives. no concept
 * of Windows as it is basically KeyedStream
 */
public class SimpleStreamingExample {

    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);
        DataStream<String> socketStream = execEnv.socketTextStream(
                "localhost", 9000);
        final DataStream<Tuple5<Long, String, String, String, Long>> selectDS = socketStream.map(new NewsFeedMapper());
        final KeyedStream<Tuple5<Long, String, String, String, Long>, Tuple> keyedDS = selectDS.keyBy(1, 2);
        final DataStream<Tuple5<Long, String, String, String, Long>> result = keyedDS.
                sum(4);
        final DataStream<Tuple3<String, String, Long>> projectedResult = result.project(1, 2, 4);
        projectedResult.print();
        execEnv.execute("Simple Streaming");
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
        final SimpleStreamingExample window = new SimpleStreamingExample();
        window.executeJob();
    }
}
