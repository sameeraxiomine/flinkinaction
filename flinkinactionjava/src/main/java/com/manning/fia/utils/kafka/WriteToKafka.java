package com.manning.fia.utils.kafka;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

/**
 * Created by hari on 7/17/16.
 */
public interface WriteToKafka extends Serializable {

    void execute(ParameterTool parameterTool) throws Exception;
}
