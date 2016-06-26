package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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

        DataStream<Tuple3<String,String,Long>> selectDS = socketStream.map(new NewsFeedMapper())
                    .project(1,2,4);

         KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS = selectDS.keyBy(0, 1);

         DataStream<Tuple3<String, String, Long>> result = keyedDS.sum(2);

        result.print();

        execEnv.execute("Simple Streaming");
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket().start();
         SimpleStreamingExample window = new SimpleStreamingExample();
        window.executeJob();
    }
}
