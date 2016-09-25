package com.manning.fia.c07;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.utils.DateUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.StringValue;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Created by hari on 9/4/16.
 */
public class NewsFeedCustomAccumulatorExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        DataSet<String> newsFeeds;
        ExecutionEnvironment execEnv;
        DataSet<Tuple5<Long, String, String, String, Long>> mapperResult;
        JobExecutionResult result;

        int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv = ExecutionEnvironment.createLocalEnvironment(parallelism);
        newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        mapperResult = newsFeeds.map(new AccumulatorMapper());
        mapperResult.print();
        result = execEnv.getLastJobExecutionResult();

        System.out.println(result.getAccumulatorResult("keywords"));


    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        NewsFeedCustomAccumulatorExample newsFeedBroadcastExample = new NewsFeedCustomAccumulatorExample();
        newsFeedBroadcastExample.executeJob(parameterTool);

    }


    public class AccumulatorMapper extends RichMapFunction<String, Tuple5<Long, String, String, String, Long>> {
        private DateUtils dateUtils = new DateUtils();
        private SetAccumulator<StringValue> keywords;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.keywords = new SetAccumulator<>();
            this.getRuntimeContext().addAccumulator("keywords", keywords);
        }

        @Override
        public Tuple5<Long, String, String, String, Long> map(String value)
                throws Exception {
            final NewsFeed newsFeed = NewsFeedParser.mapRow(value);
            final long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);
            final Tuple5<Long, String, String, String, Long> tuple5 = new Tuple5<>(newsFeed.getPageId(),
                    newsFeed.getSection(),
                    newsFeed.getSubSection(),
                    newsFeed.getTopic(),
                    timeSpent);
            String[] keywords = newsFeed.getKeywords();
            for (int i = 0; i < keywords.length; i++) {
                this.keywords.add(new StringValue(keywords[i]));
            }
            return tuple5;
        }
    }


    public static class SetAccumulator<String> implements Accumulator<String, HashSet<String>> {
        private static final long serialVersionUID = 34353564534L;

        private HashSet<String> set = new HashSet<>();

        @Override
        public void add(String t) {
            this.set.add(t);
        }

        @Override
        public HashSet<String> getLocalValue() {
            return this.set;
        }

        @Override
        public void resetLocal() {
            this.set.clear();
        }

        @Override
        public void merge(Accumulator<String, HashSet<String>> accumulator) {
            this.set.addAll(accumulator.getLocalValue());
        }

        @Override
        public Accumulator<String, HashSet<String>> clone() {
            SetAccumulator<String> result = new SetAccumulator<>();
            result.set.addAll(set);
            return result;
        }
    }



}
