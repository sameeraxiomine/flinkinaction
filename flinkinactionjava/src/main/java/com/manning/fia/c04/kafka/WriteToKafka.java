package com.manning.fia.c04.kafka;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

/**
 * Created by hari on 7/17/16.
 */
public interface WriteToKafka extends Serializable {

    void execute(ParameterTool parameterTool) throws Exception;
}
