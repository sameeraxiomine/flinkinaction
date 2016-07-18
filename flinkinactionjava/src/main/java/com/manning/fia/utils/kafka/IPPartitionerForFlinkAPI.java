package com.manning.fia.utils.kafka;

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