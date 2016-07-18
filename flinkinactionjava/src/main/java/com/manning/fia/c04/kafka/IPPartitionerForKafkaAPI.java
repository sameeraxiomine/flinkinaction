package com.manning.fia.c04.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class IPPartitionerForKafkaAPI implements Partitioner {

    public IPPartitionerForKafkaAPI(VerifiableProperties props) {

    }

    public int partition(Object key, int numberOfPartions) {
        int partition = 0;
        String ipAddress = (String) key;
        int offset = ipAddress.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt( ipAddress.substring(offset+1)) % numberOfPartions;
        }
        return partition;
    }


}