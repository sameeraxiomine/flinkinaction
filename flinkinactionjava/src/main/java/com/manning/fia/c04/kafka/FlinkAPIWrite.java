package com.manning.fia.c04.kafka;

import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 7/16/16.
 * --topic newsfeed
 * --bootstrap.servers
 * localhost:9092
 * --static true
 * --filename newsfeed6
 * --num-partions 10
 */

public class FlinkAPIWrite implements WriteToKafka {

    @Override
    public void execute(final ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(5);

        DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = 673453424234324324l;
            private List<String> newsFeeds;
            public boolean running = true;
            public boolean staticFlag = parameterTool.getBoolean("static");

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                if (staticFlag) {
                    newsFeeds = NewsFeedParser.parseData("/media/pipe/newsfeed_for_kafka");
                    for (String newsFeed : newsFeeds) {
                        ctx.collect(newsFeed);
                    }
                } else {
                    // this wil be replaced by the class for static dynamic generation.
                    // later we will add a mechanism for streaming data contiosy.
                }
            }

            @Override
            public void cancel() {

            }
        });

        // write data into Kafka
        messageStream.addSink(new FlinkKafkaProducer09<String>(parameterTool.getRequired("topic"), new SimpleStringSchema()
                , parameterTool.getProperties()));

        env.execute("Write to Kafka ");
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        new FlinkAPIWrite().execute(parameterTool);

    }
}
