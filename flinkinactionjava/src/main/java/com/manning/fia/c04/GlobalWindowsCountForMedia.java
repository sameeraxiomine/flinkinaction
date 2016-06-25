package com.manning.fia.c04;

import com.manning.fia.transformations.media.NewsFeedMapper;
import org.apache.flink.shaded.com.google.common.base.Throwables;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

/**
 * this example introduces the concept of windows assigner i.e how the window is assigned with a group of keys.
 * one key can be assigned to a set of windows.
 * https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/streaming/api/windowing/assigners/WindowAssigner.html
 * here we introduce the concept of GlobalWindows, this means how a window is assigned for a key.
 * however, there is another concept called Trigger which has the condition to define the window evaluation.
 * In GlobalWindows the default is NeverTrigger that means there is no trigger called and sum will not be evaluated.
 * so whenever you use  GlobalWindows make sure you have the custom trigger or in-built trigger
 * in this example we used an in-built trigger from flink called CountTrigger which basically kickoff the window evaluation
 * when the count of keys reaches 5.
 * Infact fyi, this is what happens when somebody uses .timeWindow(Time.Seconds(5))
 * interally window(TumblingProcessingTimeWindows) is called , which has the ProcessingTimeTrigger
 */
public class GlobalWindowsCountForMedia {

    public void executeJob() {
        try {
            final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
            final DataStream<String> socketStream = execEnv.socketTextStream("localhost", 9000);
            socketStream.map(new NewsFeedMapper())
                    .keyBy(0, 1)
                    .window(GlobalWindows.create()) //windows assigner
                    .trigger(CountTrigger.of(5)) //trigger if the keycombination count is more than 5
                    .sum(2)
                    .print();

            execEnv.execute("Global Windows with Trigger");

        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
    }

    public static void main(String[] args) throws Exception {
        final GlobalWindowsCountForMedia window = new GlobalWindowsCountForMedia();
        window.executeJob();

    }
}
