package com.manning.fia.c05;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper6;

import com.manning.fia.utils.NewsFeedSocket;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 7/05/16.
 */
public class ConnectedStreamsExample {

    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);

        DataStream<String> browserStream = execEnv.socketTextStream(
                "localhost", 9000);

//        DataStream<Tuple7<Long, String, String, String, String, Long, Long>> selectDS = browserStream
//                .map(new NewsFeedMapper5());

        DataStream<NewsFeed> selectDS = browserStream
                .map(new NewsFeedMapper6());


//        SplitStream<Tuple7<Long, String, String, String, String, Long, Long>> splitStream = selectDS.
//                split(new
//                              OutputSelector<Tuple7<Long, String, String, String, String, Long, Long>>() {
//                                  @Override
//                                  public Iterable<String> select(Tuple7<Long, String, String, String, String, Long, Long> value) {
//                                      List<String> output = new ArrayList<>();
//                                      if (value.f4.equalsIgnoreCase("mobile")) {
//                                          output.add("m");
//                                      } else {
//                                          output.add("b");
//                                      }
//                                      return output;
//                                  }
//                              });


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

//        DataStream<Tuple7<Long, String, String, String, String, Long, Long>> browserSelectDS = splitStream.select("b");

        DataStream<NewsFeed> browserSelectDS = splitStream.select("b");


//        DataStream<Tuple7<Long, String, String, String, String, Long, Long>> mobileSelectDS = splitStream.select("m");

        DataStream<NewsFeed> mobileSelectDS = splitStream.select("m");


//        ConnectedStreams<Tuple7<Long, String, String, String, String, Long, Long>, Tuple7<Long, String, String, String, String, Long, Long>>
//                connectedStreams = browserSelectDS.connect(mobileSelectDS);

        ConnectedStreams<NewsFeed, NewsFeed> connectedStreams = browserSelectDS.connect(mobileSelectDS);

        //        connectedStreams.map(new Tuple7CoMapFunction()).print();

        connectedStreams.map(new NewsFeedNewsFeedCoMapFunction()).print();

        
        execEnv.execute("ConnectedStreamsExample");
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed4", 100, 9000).start();
        ConnectedStreamsExample window = new ConnectedStreamsExample();
        window.executeJob();
    }

    private static class Tuple7CoMapFunction implements CoMapFunction<Tuple7<Long, String, String, String, String, Long, Long>,
            Tuple7<Long, String, String, String, String, Long, Long>, Object> {

        @Override
        public Tuple3<String, String, Long> map1(Tuple7<Long, String, String, String, String, Long, Long> value)
                throws Exception {
            return new Tuple3<>(value.f1, value.f2, value.f6 - value.f5);
        }

        @Override
        public Tuple3<String, String, Long> map2(Tuple7<Long, String, String, String, String, Long, Long> value)
                throws Exception {
            return new Tuple3<>(value.f1, value.f2, value.f6 - value.f5);
        }
    }

    private static class NewsFeedNewsFeedCoMapFunction implements CoMapFunction<NewsFeed, NewsFeed, NewsFeed> {
        @Override
        public NewsFeed map1(NewsFeed value) throws Exception {
            return value;
        }

        @Override
        public NewsFeed map2(NewsFeed value) throws Exception {
            return value;
        }
    }
}
