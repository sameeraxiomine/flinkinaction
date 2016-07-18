package com.manning.fia.c04;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper6;
import com.manning.fia.utils.NewsFeedSocket;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        execEnv.execute("Split Streams Example");
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed4", 100, 9000).start();
        SplitStreamsExample window = new SplitStreamsExample();
        window.executeJob();
    }


}
