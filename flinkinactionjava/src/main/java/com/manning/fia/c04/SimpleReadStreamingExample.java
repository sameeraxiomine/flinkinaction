package com.manning.fia.c04;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.utils.NewsFeedDataSource;

public class SimpleReadStreamingExample {
    private void executeJob(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment execEnv;
        DataStream<String> dataStream;
        DataStream<Tuple3<String, String, Long>> selectDS;

        execEnv = StreamExecutionEnvironment.getExecutionEnvironment(); 

        execEnv.setParallelism(parameterTool.getInt("parallelism", 1)); 
        dataStream = execEnv.addSource(
                NewsFeedDataSource.getKafkaDataSource(parameterTool));

        
        selectDS  = dataStream.map(new NewsFeedMapper())                            
                              .project(1, 2, 4);                        
        selectDS.print();                                               
        execEnv.execute("Simple Streaming");                            
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SimpleReadStreamingExample window = new SimpleReadStreamingExample();
        window.executeJob(parameterTool);
    }
}
