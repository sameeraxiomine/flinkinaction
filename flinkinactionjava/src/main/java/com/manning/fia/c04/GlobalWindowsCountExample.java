package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.utils.NewsFeedDataSource;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
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
 *
 * * if it is kafka
 * --isKafka true --topic newsfeed --bootstrap.servers localhost:9092 --num-partions 10 --zookeeper.connect
 * localhost:2181 --group.id myconsumer --parallelism numberofpartions
 * else
 * don't need to send anything.
 * one of the optional parameters for both the sections are
 * --fileName /media/pipe/newsfeed_for_count_windows
 */
public class GlobalWindowsCountExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(1);
        execEnv.setParallelism(parameterTool.getInt("parallelism", 1));
        final DataStream<String> dataStream;
        boolean isKafka = parameterTool.getBoolean("isKafka", false);
        if (isKafka) {
            dataStream = execEnv.addSource(NewsFeedDataSource.getKafkaDataSource(parameterTool));
        } else {
            dataStream = execEnv.addSource(NewsFeedDataSource.getCustomDataSource(parameterTool));
        }

        DataStream<Tuple3<String, String, Long>> selectDS = dataStream
                .map(new NewsFeedMapper()).project(1, 2, 4);

        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedDS = selectDS
                .keyBy(0, 1);

        WindowedStream<Tuple3<String, String, Long>, Tuple, GlobalWindow> windowedStream = keyedDS
                .window(GlobalWindows.create());

        windowedStream.trigger(PurgingTrigger.of(CountTrigger.of(3)));

        DataStream<Tuple3<String, String, Long>> result = windowedStream
                .sum(2);
        result.print();

        execEnv.execute("Global Windows with Trigger");
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        GlobalWindowsCountExample window = new GlobalWindowsCountExample();
        window.executeJob(parameterTool);

    }
}
