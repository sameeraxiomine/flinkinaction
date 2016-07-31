package com.manning.fia.c04;

import com.manning.fia.transformations.media.ExtractIPAddressMapper;
import com.manning.fia.transformations.media.NewsFeedMapper;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by hari on 7/28/16.
 */
public class DataStreamGenerator {


    public static KeyedStream<Tuple3<String, String, Long>, Tuple> getC04KeyedStream(StreamExecutionEnvironment execEnv,
                                                                              ParameterTool parameterTool) {
        execEnv.setParallelism(parameterTool.getInt("parallelism", 1));

        DataStream<String> dataStream;
        DataStream<Tuple3<String, String, Long>> selectDS;
        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS;

        dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

        selectDS = dataStream.map(new NewsFeedMapper()).project(1, 2, 4);

        keyedDS = selectDS.keyBy(0, 1);

        return keyedDS;
    }

    static DataStream<Tuple3<String, String, Long>> getC04ProjectedDataStream(StreamExecutionEnvironment execEnv,
                                                                              ParameterTool parameterTool) {

        execEnv.setParallelism(parameterTool.getInt("parallelism", 1));

        DataStream<String> dataStream;
        DataStream<Tuple3<String, String, Long>> selectDS;

        dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

        selectDS = dataStream.map(new NewsFeedMapper()).project(1, 2, 4);

        return selectDS;
    }

    static DataStream<String> getC04BotDataStream(StreamExecutionEnvironment execEnv,
                                                  ParameterTool parameterTool) {

        execEnv.setParallelism(parameterTool.getInt("parallelism", 1));

        DataStream<String> dataStream;
        DataStream<String> selectDS;

        dataStream = execEnv.addSource(DataSourceFactory.getDataSource(parameterTool));

        selectDS = dataStream.map(new ExtractIPAddressMapper());

        return selectDS;
    }
}
