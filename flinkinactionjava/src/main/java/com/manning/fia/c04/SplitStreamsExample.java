package com.manning.fia.c04;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper6;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 7/05/16.
 */
public class SplitStreamsExample {

    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);

        DataStream<String> browserStream = execEnv.socketTextStream(
                "localhost", 9000);


        DataStream<NewsFeed> selectDS = browserStream
                .map(new NewsFeedMapper6());


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

        execEnv.execute("SplitStreamsExample");
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed4", 100, 9000).start();
        SplitStreamsExample window = new SplitStreamsExample();
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

    private static class NewsFeedNewsFeedCoMapFunction implements CoMapFunction<NewsFeed, NewsFeed, Object> {
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
