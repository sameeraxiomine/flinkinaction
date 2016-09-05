package com.manning.fia.c07;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.model.media.Page;
import com.manning.fia.transformations.media.DomainObjectBasedPageMapper;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.transformations.media.PageParser;
import com.manning.fia.utils.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeutils.base.LongComparator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hari on 9/4/16.
 */
public class NewsFeedBroadcastExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        DataSet<Tuple6<Long, String, String, String, String, Long>> result;
        int parallelism = parameterTool.getInt("parallelism", 1);

        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(parallelism);

        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<String> pageInformationValues = execEnv
                .fromCollection(PageParser.parseData());

        DataSet<Page> pageInformationDataSet = pageInformationValues
                .map(new DomainObjectBasedPageMapper());

        result = newsFeeds.map(new BroadcastMapper()).withBroadcastSet(pageInformationDataSet, "pageInformation");
        result.print();
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        NewsFeedBroadcastExample newsFeedBroadcastExample = new NewsFeedBroadcastExample();
        newsFeedBroadcastExample.executeJob(parameterTool);

    }

    private static class BroadcastMapper extends RichMapFunction<String,
            Tuple6<Long,
                    String, String, String, String, Long>> {
        Map<Long, String> map = new HashMap<>();
        DateUtils dateUtils = new DateUtils();

        @Override
        public void open(Configuration parameters) throws Exception {
            Collection<Page> collection = getRuntimeContext().getBroadcastVariable("pageInformation");
            for (Page page : collection) {
                map.put(page.getId(), page.getAuthor());
            }
        }

        @Override
        public Tuple6<Long, String, String, String, String, Long> map(String value) throws Exception {
            final NewsFeed newsFeed = NewsFeedParser.mapRow(value);
            final long timeSpent = dateUtils.getTimeSpentOnPage(newsFeed);
            final Tuple6<Long, String, String, String, String, Long> tuple6 = new Tuple6<>(newsFeed.getPageId(),
                    newsFeed.getSection(),
                    newsFeed.getSubSection(),
                    newsFeed.getTopic(),
                    map.get(newsFeed.getPageId()),
                    timeSpent);
            return tuple6;

        }
    }
}
