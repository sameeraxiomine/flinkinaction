package com.manning.fia.utils;

import com.manning.fia.utils.custom.NewsFeedCustomDataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Created by hari on 7/28/16.
 */
public class DataSourceFactory {

    public static SourceFunction<String> getDataSource(ParameterTool parameterTool) {
        if (parameterTool.getBoolean("isKafka",false)) {
            return new FlinkKafkaConsumer09<String>(
                    parameterTool.getRequired("topic"),
                    new SimpleStringSchema(),
                    parameterTool.getProperties());
        }
        return new NewsFeedCustomDataSource(parameterTool);
    }
}
