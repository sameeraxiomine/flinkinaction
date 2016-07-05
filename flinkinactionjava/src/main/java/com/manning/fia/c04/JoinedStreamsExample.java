package com.manning.fia.c04;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper6;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 7/05/16.
 */
public class JoinedStreamsExample {

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


        browserSelectDS.join(mobileSelectDS)
                .where(new NewsFeedStringKeySelector()).
                equalTo(new NewsFeedStringKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new NewsFeedNewsFeedStringJoinFunction())
                .print();


        execEnv.execute("JoinedStreamsExample");


    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed4", 0, 9000).start();
        JoinedStreamsExample window = new JoinedStreamsExample();
        window.executeJob();
    }


    private static class NewsFeedNewsFeedStringJoinFunction implements JoinFunction<NewsFeed, NewsFeed, String> {
        @Override
        public String join(NewsFeed first, NewsFeed second) throws Exception {
            return first.getEventId() + ":" + second.getEventId();
        }
    }

    private static class NewsFeedStringKeySelector implements KeySelector<NewsFeed, String> {
        @Override
        public String getKey(NewsFeed value) throws Exception {
            return value.getSection();
        }
    }
}
