package com.manning.fia.c03;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.model.media.Page;
import com.manning.fia.transformations.media.ComputeTimeSpentPerSectionAndSubSection;
import com.manning.fia.transformations.media.DomainObjectBasedNewsFeedMapper;
import com.manning.fia.transformations.media.DomainObjectBasedPageMapper;
import com.manning.fia.transformations.media.FilterOnTimeSpentPerPage;
import com.manning.fia.transformations.media.GroupReduceComputeTimeSpentPerSectionAndSubSection;
import com.manning.fia.transformations.media.MapPartitionNewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedMapper2;
import com.manning.fia.transformations.media.NewsFeedMapper8;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.transformations.media.PageMapper;
import com.manning.fia.transformations.media.PageParser;
import com.manning.fia.transformations.media.SectionSubSectionKeySelector;
import com.manning.fia.transformations.media.SortGroupReduceForPriorPageId;
import com.manning.fia.utils.DateUtils;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

/**
 * base file for handling the batch use case.
 */
public class MediaBatchTransformations {

    private static int DEFAULT_LOCAL_PARALLELISM = 1;

    /*
     * Map. In the mapper TimeSpent is calculated.
     * Note here,we have writtern a mapper that implements MapFunction.
     *
     */
    public static void usingMap() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
                .map(new NewsFeedMapper());
        result.print();
    }

    /*
     * One can also project what the user needs from Mapper, let us say if the
     * application also wants distinct section,subsection along the entire news
     * Feed. Project This can be applied only on Tuples,printing
     * section,subection distinct values
     */
    public static void usingProject() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple2<String, String>> result = newsFeeds.map(
                new NewsFeedMapper()).project(1, 2);
        result.print();

    }

    public static void usingProjectWithTypeHint() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple2<String, String>> result = newsFeeds
                .map(new NewsFeedMapper())
                .<Tuple2<String, String>>project(1, 2).distinct();
        result.print();
    }

    /*
     * Illustration of filter, let us say if the news agency wants to see the
     * section,subsection that has as a reading rate of more than 6 second.
     * Note here, we wrote a class that implements FilterFunction to implement this logic.
     */
    public static void usingFilter() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
                .map(new NewsFeedMapper()).filter(
                        new FilterOnTimeSpentPerPage());
        result.print();
    }

    /*
     * another way of doing map
     * Map Partition.
     * Note here, we wrote a class that implements MapPartitionFunction to implement this logic.
     *
     */
    public static void usingMapPartition() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
                .mapPartition(new MapPartitionNewsFeedMapper());
        result.print();
    }


    /*
     * now if the user agency wants to calculate the entire time spent by
     * section,subsection then reduce is used by summing up the timespent.
     * This method will call the mapper first and later we call reduce.
     */
    public static void usingReduce() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple3<String, String, Long>> result = newsFeeds
                .map(new NewsFeedMapper())
                .<Tuple3<String, String, Long>>project(1, 2, 4).groupBy(0, 1)
                .reduce(new ComputeTimeSpentPerSectionAndSubSection());
        result.print();
    }

    /*
     * another way of doing reduce as illustrated above using Aggregegation.
     * Reduce by introducing aggregations, same as Above
     */
    public static void usingAggregation() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple3<String, String, Long>> result = newsFeeds.map(new NewsFeedMapper())
                .<Tuple3<String, String, Long>>project(1, 2, 4)
                .groupBy(0, 1)
                .aggregate(Aggregations.SUM, 2);
        result.print();

    }

    public static void usingMultipleAggregations() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple5<String, String, Long, Long, Long>> result = newsFeeds.map(new NewsFeedMapper2())
                .groupBy(0, 1)
                .aggregate(Aggregations.MIN, 2)
                .and(Aggregations.MAX, 3)
                .and(Aggregations.SUM, 4);
        result.print();
    }

    /*
     * another way of doing reduce as illustrated above using some util
     *  Reduce by using util method sum, same as Above
     */
    public static void usingSum() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        newsFeeds.map(new NewsFeedMapper()).project(1, 2, 4).groupBy(0, 1)
                .sum(2).print();

    }

    /*
     * Introducing groupBy with KeySelector.
     * using keyselector, another way of selecting keys and using group reduce to have the same result as above.
     */

    public static void usingKeySelectorAndGroupReduce() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeedValues = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<NewsFeed> newsFeeds = newsFeedValues
                .map(new DomainObjectBasedNewsFeedMapper());

        DataSet<Tuple3<String, String, Long>> result = newsFeeds.groupBy(
                new SectionSubSectionKeySelector()).reduceGroup(
                new GroupReduceComputeTimeSpentPerSectionAndSubSection());
        result.print();

    }

    /*
     * Group Reduce using Sorting.
     */
    public static void usingGroupReduceSortedKeys() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData("/media/pipe/newsfeed7"));
        newsFeeds.map(new NewsFeedMapper8())
                .groupBy(0)
                .sortGroup(2, Order.ASCENDING)
                .reduceGroup(new SortGroupReduceForPriorPageId())
                .print();
    }

    /*
     * using keyselector, another way of selecting keys,sort key and using
     * group reduce to have the same result as above
     */

    public static void usingGroupReduceSortedKeysWithKeySelector()
            throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeedValues = execEnv
                .fromCollection(NewsFeedParser.parseData());
        final DataSet<NewsFeed> newsFeeds = newsFeedValues
                .map(new DomainObjectBasedNewsFeedMapper());
        newsFeeds
                .groupBy(new KeySelector<NewsFeed, String>() {
                    @Override
                    public String getKey(NewsFeed value) throws Exception {
                        return value.getSection();
                    }
                })
                .sortGroup(new KeySelector<NewsFeed, String>() {
                    @Override
                    public String getKey(NewsFeed value) throws Exception {
                        return value.getSubSection();
                    }
                }, Order.ASCENDING)
                .reduceGroup(
                        new GroupReduceFunction<NewsFeed, Tuple3<String, String, Long>>() {
                            private DateUtils dateUtils = new DateUtils();

                            @Override
                            public void reduce(Iterable<NewsFeed> newsFeeds,
                                               Collector<Tuple3<String, String, Long>> out)
                                    throws Exception {
                                long total = 0l;
                                String section = null;
                                String subSection = null;
                                for (NewsFeed feed : newsFeeds) {
                                    section = feed.getSection();
                                    subSection = feed.getSubSection();
                                    final long timeSpent = dateUtils
                                            .getTimeSpentOnPage(feed);
                                    total = total + timeSpent;
                                }
                                out.collect(new Tuple3<>(section, subSection,
                                        total));
                            }
                        }).print();

    }

    /*
     * Group Combine
     * Same concept as combiner in map reduce world.
     */
    public static void usingGroupCombine() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeedValues = execEnv
                .fromCollection(NewsFeedParser.parseData());
        DataSet<NewsFeed> newsFeeds = newsFeedValues
                .map(new DomainObjectBasedNewsFeedMapper());

        DataSet<Tuple3<String, String, Long>> combined = newsFeeds
                .groupBy("section", "subSection")
                .combineGroup(
                        new GroupCombineFunction<NewsFeed, Tuple3<String, String, Long>>() {
                            DateUtils dateUtils = new DateUtils();

                            @Override
                            public void combine(Iterable<NewsFeed> values,
                                                Collector<Tuple3<String, String, Long>> out)
                                    throws Exception {
                                long total = 0l;
                                String section = null;
                                String subSection = null;
                                for (NewsFeed feed : values) {
                                    section = feed.getSection();
                                    subSection = feed.getSubSection();
                                    final long timeSpent = dateUtils
                                            .getTimeSpentOnPage(feed);
                                    total = total + timeSpent;
                                }
                                out.collect(new Tuple3<>(section, subSection,
                                        total));
                            }
                        });
        DataSet<Tuple3<String, String, Long>> output =
                combined.groupBy(0, 1)
                        .reduce(new ComputeTimeSpentPerSectionAndSubSection());
        output.print();
    }

    /*
     * basic Join i.e default join which provides Tuple2 with newsfeed and
     * page
     */

    public static void usingBasicJoin() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeedValues = execEnv
                .fromCollection(NewsFeedParser.parseData());
        DataSet<String> pageInformationValues = execEnv
                .fromCollection(PageParser.parseData());
        DataSet<NewsFeed> newsFeeds = newsFeedValues
                .map(new DomainObjectBasedNewsFeedMapper());
        DataSet<Page> pageInformationDataSet = pageInformationValues
                .map(new DomainObjectBasedPageMapper());
        DataSet<Tuple2<NewsFeed, Page>> joinDataSet = newsFeeds
                .join(pageInformationDataSet).where("pageId").equalTo("id");
        joinDataSet.print();
    }

    /*
     * basic Join with a Tuple and an Object
     */

    public static void usingBasicJoinWithTupleAndObject() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<String> pageInformationValues = execEnv
                .fromCollection(PageParser.parseData());
        DataSet<Page> pageInformationDataSet = pageInformationValues
                .map(new DomainObjectBasedPageMapper());
        DataSet<Tuple5<Long, String, String, String, Long>> tuple5DataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple2<Tuple5<Long, String, String, String, Long>, Page>> joinDataSet =
                tuple5DataSet.join(pageInformationDataSet)
                        .where("f0").equalTo("id");
        joinDataSet.print();
    }

    /*
     * basic Join with a Tuple and an Tuple
     */

    public static void usingBasicJoinWithTupleAndTuple() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<String> pageInformationDataSet = execEnv
                .fromCollection(PageParser.parseData());
        DataSet<Tuple5<Long, String, String, String, Long>> tuple5DataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> tuple3DataSet = pageInformationDataSet
                .map(new PageMapper());
        DataSet<Tuple2<Tuple5<Long, String, String, String, Long>, Tuple3<String, String, Long>>> joinDataSet =
                tuple5DataSet.join(tuple3DataSet).where("f0").equalTo("f2");
        joinDataSet.print();
    }

    /*
     * 12 join function with projection. This will give us the topic,timespent
     * on a topic for each news feed and the page URL.
     */

    public static void usingJoinWithProjection() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<String> pageInformationDataSet = execEnv
                .fromCollection(PageParser.parseData());
        DataSet<Tuple5<Long, String, String, String, Long>> tuple5DataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> tuple3DataSet = pageInformationDataSet
                .map(new PageMapper());
        DataSet<Tuple3<String, String, Long>> joinDataSet = tuple5DataSet.join(tuple3DataSet)
                .where("0").equalTo("2")
                .projectFirst(2, 3).projectSecond(0);
        joinDataSet.print();
    }

    /*
     * join with hint. Generally the news feed is basically a large table,
     * on the other hand pageInformation is small
     */
    public static void usingJoinWithHint() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<String> pageInformationDataSet = execEnv
                .fromCollection(PageParser.parseData());
        DataSet<Tuple5<Long, String, String, String, Long>> largeDataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> smallDataSet = pageInformationDataSet
                .map(new PageMapper());
        largeDataSet
                .join(smallDataSet,
                        JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                .where("0").equalTo("2").print();
        smallDataSet
                .join(largeDataSet,
                        JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
                .where("2").equalTo("0").print();
    }

    /*
     * join with hint. Generally the news feed is basically a large table,
     * on the other hand pageInformation is small
     */

    public static void usingJoinWithHintUtility() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());

        DataSet<String> pageInformationDataSet = execEnv
                .fromCollection(PageParser.parseData());

        DataSet<Tuple5<Long, String, String, String, Long>> largeDataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> smallDataSet = pageInformationDataSet
                .map(new PageMapper());
        largeDataSet.joinWithTiny(smallDataSet).where("0").equalTo("2").print();
        smallDataSet.joinWithHuge(largeDataSet).where("2").equalTo("0").print();
    }


    public static void main(String[] args) throws Exception {
        MediaBatchTransformations.usingMap();
//        MediaBatchTransformations.usingProject();
//        MediaBatchTransformations.usingFilter();
//        MediaBatchTransformations.usingMapPartition();
//        MediaBatchTransformations.usingReduce();
//        MediaBatchTransformations.usingAggregation();
//        MediaBatchTransformations.usingSum();
//        MediaBatchTransformations.usingGroupReduceSortedKeys();
//        MediaBatchTransformations.usingKeySelectorAndGroupReduce();
//        MediaBatchTransformations.usingGroupReduceSortedKeysWithKeySelector();
//        MediaBatchTransformations.usingGroupCombine();
//        MediaBatchTransformations.usingBasicJoin();
//        MediaBatchTransformations.usingBasicJoinWithTupleAndObject();
//        MediaBatchTransformations.usingBasicJoinWithTupleAndTuple();
//        MediaBatchTransformations.usingJoinWithProjection();
//        MediaBatchTransformations.usingJoinWithHint();
//        MediaBatchTransformations.usingJoinWithHintUtility();
    }

}

