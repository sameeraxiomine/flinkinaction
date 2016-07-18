package com.manning.fia.c04.kafka;

import com.manning.fia.transformations.media.NewsFeedParser;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;

import java.io.Serializable;

public class IPPartitionerForFlinkAPI extends KafkaPartitioner<String> implements Serializable {

    private final int expectedPartitions;

    public IPPartitionerForFlinkAPI(int expectedPartitions) {
        this.expectedPartitions = expectedPartitions;
    }

    @Override
    public int partition(String messsage, byte[] key, byte[] value, int numberOfPartions) {
        if (numberOfPartions != expectedPartitions) {
            throw new IllegalArgumentException("parallelism not right");
        }
//        return NewsFeedParser.mapRow(messsage).getUser().getIpAddress();
        return 0;
    }
}