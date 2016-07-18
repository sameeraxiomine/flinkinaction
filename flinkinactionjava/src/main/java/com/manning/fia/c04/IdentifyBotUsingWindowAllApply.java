package com.manning.fia.c04;

import com.manning.fia.transformations.media.ExtractIPAddressMapper;
import com.manning.fia.utils.NewsFeedDataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Created by hari on 6/26/16.
 *  * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed_bot_identifier

 */
public class IdentifyBotUsingWindowAllApply {
    private void executeJob(ParameterTool parameterTool) throws Exception {

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        execEnv.setParallelism(parameterTool.getInt("parallelism", 1));

        final DataStream<String> dataStream;
        boolean isKafka = parameterTool.getBoolean("isKafka", false);
        if (isKafka) {
            dataStream = execEnv.addSource(NewsFeedDataSource.getKafkaDataSource(parameterTool));
        } else {
            dataStream = execEnv.addSource(NewsFeedDataSource.getCustomDataSource(parameterTool));
        }

        DataStream<String> selectDS = dataStream
                .map(new ExtractIPAddressMapper());
        AllWindowedStream<String, TimeWindow> ws1=
                selectDS.timeWindowAll(Time.seconds(2),Time.seconds(1));
        DataStream<String> result1 = ws1.apply(new AllApplyFunction());
        result1.print();
        execEnv.execute("Processing Time Window All Apply");
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        IdentifyBotUsingWindowAllApply window = new IdentifyBotUsingWindowAllApply();
        window.executeJob(parameterTool);

    }
}
