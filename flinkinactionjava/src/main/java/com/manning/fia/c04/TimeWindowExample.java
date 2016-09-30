package com.manning.fia.c04;

/**
 * * * * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed3
 */

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


public class TimeWindowExample {
    private void executeJob(ParameterTool parameterTool) throws Exception {

        StreamExecutionEnvironment execEnv;
        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS;
        WindowedStream<Tuple3<String, String, Long>, Tuple, TimeWindow> windowedStream;
        DataStream<Tuple3<String, String, Long>> result;

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        keyedDS = DataStreamGenerator.getC04KeyedStream(execEnv, parameterTool);
        
        int tumblingInterval = parameterTool.getInt("tumbleInterval",4);
        int slidingInterval = parameterTool.getInt("slideInterval",0);
        if(slidingInterval>0){
        	windowedStream = keyedDS.timeWindow(Time.seconds(tumblingInterval),Time.seconds(slidingInterval));	
        }else{
        	windowedStream = keyedDS.timeWindow(Time.seconds(tumblingInterval));
        }        

        result = windowedStream.sum(2);

        result.print();

        execEnv.execute("Processing Time Window");

    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        TimeWindowExample window = new TimeWindowExample();
        window.executeJob(parameterTool);

    }
}
