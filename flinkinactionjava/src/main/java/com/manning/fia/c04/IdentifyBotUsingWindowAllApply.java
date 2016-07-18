package com.manning.fia.c04;

import com.manning.fia.transformations.media.ExtractIPAddressMapper;

import com.manning.fia.utils.NewsFeedSocket;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * Created by hari on 6/26/16.
 */
public class IdentifyBotUsingWindowAllApply {
    public void executeJob() throws Exception {

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);        
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<String> socketStream = execEnv.socketTextStream("localhost",
                9000);
        DataStream<String> selectDS = socketStream
                .map(new ExtractIPAddressMapper());
        AllWindowedStream<String, TimeWindow> ws1=
                selectDS.timeWindowAll(Time.seconds(2),Time.seconds(1));
        DataStream<String> result1 = ws1.apply(new AllApplyFunction());
        result1.print();
        execEnv.execute("Processing Time Window All Apply");
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed_bot_identifier",9000).start();
        IdentifyBotUsingWindowAllApply window = new IdentifyBotUsingWindowAllApply();
        window.executeJob();

    }
}
