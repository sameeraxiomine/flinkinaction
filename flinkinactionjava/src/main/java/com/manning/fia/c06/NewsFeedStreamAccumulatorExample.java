package com.manning.fia.c06;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.utils.DataSourceFactory;
import com.manning.fia.utils.DateUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;

/**
 * Created by hari on 9/4/16.
 */
public class NewsFeedStreamAccumulatorExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        DataStream<String> newsFeeds;
        Configuration config = new Configuration();
        config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        final int parallelism = parameterTool.getInt("parallelism", 1);
        execEnv.setParallelism(parallelism);


        newsFeeds = execEnv.addSource(new CustomDataSource(parameterTool));
        newsFeeds.map(new AccumulatorMapper());

        execEnv.execute();
    }

    public class CustomDataSource implements SourceFunction<String> {


        private ParameterTool parameterTool;

        public CustomDataSource(ParameterTool parameterTool) {
            this.parameterTool = parameterTool;
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            long startTime = System.currentTimeMillis();
            while (true) {
                final String fileName = parameterTool.get("fileName", "/media/pipe/newsfeed");
                final List<String> list = NewsFeedParser.parseData(fileName);
                final int threadSleepInterval = parameterTool.getInt("threadSleepInterval", 0);
                for (String newsFeed : list) {
                    sourceContext.collect(newsFeed);
                    Thread.currentThread().sleep(threadSleepInterval);
                }
                long endTime = System.currentTimeMillis();
                long diff=endTime-startTime;
                if (diff>=3000){
                    break;
                }
            }
        }

        @Override
        public void cancel() {

        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        NewsFeedStreamAccumulatorExample newsFeedBroadcastExample = new NewsFeedStreamAccumulatorExample();
        newsFeedBroadcastExample.executeJob(parameterTool);

    }

    public class AccumulatorMapper extends RichMapFunction<String, Tuple3<String, String, Long>> {
        private DateUtils dateUtils = new DateUtils();
        private IntCounter counter = new IntCounter();


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            getRuntimeContext().addAccumulator("pageViews", counter);
        }

        @Override
        public Tuple3<String, String, Long> map(String value)
                throws Exception {
            final NewsFeed newsFeed = NewsFeedParser.mapRow(value);
            counter.add(1);
            final long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);
            return new Tuple3<>(newsFeed.getSection(), newsFeed.getSubSection(), timeSpent);
        }

    }


}
