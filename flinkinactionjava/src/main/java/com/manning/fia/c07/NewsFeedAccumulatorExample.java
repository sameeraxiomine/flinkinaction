package com.manning.fia.c07;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.ComputeTimeSpentPerSectionAndSubSection;
import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.utils.DateUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 * Created by hari on 9/4/16.
 */
public class NewsFeedAccumulatorExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        DataSet<String> newsFeeds;
        ExecutionEnvironment execEnv;
        DataSet<Tuple3<String, String, Long>> reducerResult;
        JobExecutionResult result;

        int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv = ExecutionEnvironment.createLocalEnvironment(parallelism);
        newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        reducerResult=newsFeeds.map(new NewsFeedMapper())
                .<Tuple3<String, String, Long>>project(1, 2, 4).groupBy(0, 1)
                .reduce(new AccumulatorReducer());
        reducerResult.print();
        result = execEnv.getLastJobExecutionResult();

        System.out.println(result.getAccumulatorResult("pageViewsBySectionSubSection"));



    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        NewsFeedAccumulatorExample newsFeedBroadcastExample = new NewsFeedAccumulatorExample();
        newsFeedBroadcastExample.executeJob(parameterTool);

    }

    public class AccumulatorReducer extends RichReduceFunction<Tuple3<String, String, Long>> {

        private IntCounter counter = new IntCounter();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("pageViewsBySectionSubSection", counter);

        }

        @Override
        public Tuple3<String, String, Long> reduce(
                Tuple3<String, String, Long> value1,
                Tuple3<String, String, Long> value2) throws Exception {

            counter.add(1);
            return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
        }
    }

}
