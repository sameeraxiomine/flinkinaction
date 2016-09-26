package com.manning.fia.newc06;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper6;
import com.manning.fia.utils.NewsFeedDataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 7/05/16.
 /**
 * * * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed4 --threadSleepInterval 100
 */


public class SplitStreamsExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {
        final DataStream<String> dataStream;
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        final int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv.setParallelism(parallelism);
        boolean isKafka = parameterTool.getBoolean("isKafka", false);
        if (isKafka) {
            dataStream = execEnv.addSource(NewsFeedDataSource.getKafkaDataSource(parameterTool));
        } else {
            dataStream = execEnv.addSource(NewsFeedDataSource.getCustomDataSource(parameterTool));
        }

        DataStream<NewsFeed> selectDS = dataStream.map(new NewsFeedMapper6());

        SplitStream<NewsFeed> splitStream = selectDS.
                split(new
                              OutputSelector<NewsFeed>() {
                                  @Override
                                  public Iterable<String> select(NewsFeed value) {
                                      List<String> output = new ArrayList<>();
                                      if (value.getDeviceType().equalsIgnoreCase("mobile")) {
                                          output.add("m");
                                      } else {
                                          output.add("b");
                                      }
                                      return output;
                                  }
                              });


        DataStream<NewsFeed> browserSelectDS = splitStream.select("b");

        DataStream<NewsFeed> mobileSelectDS = splitStream.select("m");

        browserSelectDS.print();

        mobileSelectDS.print();

        execEnv.execute("Split Streams Example");
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        SplitStreamsExample window = new SplitStreamsExample();
        window.executeJob(parameterTool);
    }


}
