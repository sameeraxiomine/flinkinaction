package com.manning.fia.c04.kafka;

import com.manning.fia.transformations.media.NewsFeedParser;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 7/16/16.
 * --topic newsfeed1
 * --metadata.broker.list broker1:9092,broker2:9092
 * serializer.class:kafka.serializer.StringEncoder
 * --partitioner.class example.producer.SimplePartitioner
 * --request.required.acks 1
 * --static true
 * --filename newsfeed6
 * --num-partions 10
 */

public class KafkaAPIWrite implements WriteToKafka {

    @Override
    public void execute(final ParameterTool parameterTool) throws Exception {
        final ProducerConfig config = new ProducerConfig(parameterTool.getProperties());
        final Producer<String, String> producer = new Producer<String, String>(config);
        final boolean staticFlag = parameterTool.getBoolean("static");
        List<String> newsFeeds;
        if (staticFlag) {
            newsFeeds = NewsFeedParser.parseData("/media/pipe/newsfeed_for_kafka");
            for (String newsFeed : newsFeeds) {
                final String ipAddress = NewsFeedParser.mapRow(newsFeed).getUser().getIpAddress();
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                        parameterTool.getRequired("topic"),
                        ipAddress,
                        newsFeed);
                producer.send(data);
            }
        } else {
            newsFeeds = new ArrayList<String>();
            // this wil be replaced by the class for static dynamic generation.
            // later we will add a mechanism for streaming data contiosy.
        }
        producer.close();

    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        new KafkaAPIWrite().execute(parameterTool);
    }
}

