package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * this example introduces the concept of windows assigner i.e how the window is
 * assigned with a group of keys. one key can be assigned to a set of windows.
 * https
 * ://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink
 * /streaming/api/windowing/assigners/WindowAssigner.html here we introduce the
 * concept of GlobalWindows, this means how a window is assigned for a key.
 * however, there is another concept called Trigger which has the condition to
 * define the window evaluation. In GlobalWindows the default is NeverTrigger
 * that means there is no trigger called and sum will not be evaluated. so
 * whenever you use GlobalWindows make sure you have the custom trigger or
 * in-built trigger in this example we used an in-built trigger from flink
 * called CountTrigger which basically kickoff the window evaluation when the
 * count of keys reaches 5. Infact fyi, this is what happens when somebody uses
 * .timeWindow(Time.Seconds(5)) interally window(TumblingProcessingTimeWindows)
 * is called , which has the ProcessingTimeTrigger
 */
public class GlobalWindowsCountExample {

    public void executeJob() throws Exception {
         StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);

         DataStream<String> socketStream = execEnv.socketTextStream(
                "localhost", 9000);

         DataStream<Tuple3<String, String, Long>> selectDS = socketStream
                .map(new NewsFeedMapper()).project(1, 2, 4);

         KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS = selectDS
                .keyBy(0, 1);

         WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream = keyedDS
                .window(GlobalWindows.create()); // windows assigner

        windowedStream.trigger(CountTrigger.of(2));// trigger if the
                                                   // keycombination count is
                                                   // more than 5

         DataStream<Tuple3<String, String, Long>> result = windowedStream
                .sum(2);

        result.print();

        execEnv.execute("Global Windows with Trigger");
    }

    public static void main(String[] args) throws Exception {
        new NewsFeedSocket("/media/pipe/newsfeed_for_count_windows").start();
         GlobalWindowsCountExample window = new GlobalWindowsCountExample();
        window.executeJob();

    }
}
