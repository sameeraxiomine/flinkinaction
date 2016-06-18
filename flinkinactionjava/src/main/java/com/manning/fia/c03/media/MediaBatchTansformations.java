package com.manning.fia.c03.media;

import java.util.Arrays;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.operators.JoinOperator.ProjectJoin;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import com.manning.fia.c03.SampleData;
import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.model.media.Page;
import com.manning.fia.transformations.MapTokenizeAndComputeTransactionValue2;
import com.manning.fia.transformations.SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId;
import com.manning.fia.transformations.media.ComputeTimeSpentPerSectionAndSubSection;
import com.manning.fia.transformations.media.DomainObjectBasedNewsFeedMapper;
import com.manning.fia.transformations.media.DomainObjectBasedPageInformationParser;
import com.manning.fia.transformations.media.FilterOnTimeSpentPerPage;
import com.manning.fia.transformations.media.GroupReduceComputeTimeSpentPerSectionAndSubSection;
import com.manning.fia.transformations.media.MapPartitionNewsFeedMapper;
import com.manning.fia.transformations.media.PageMapper;
import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedMapper2;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.transformations.media.PageParser;
import com.manning.fia.transformations.media.SectionSubSectionKeySelector;
import com.manning.fia.transformations.media.SortedGroupReduceComputeTimeSpentBySectionAndSubSection;

public class MediaBatchTansformations {

    // We can talk about the way we can run our program in local mode and remote
    // mode in appendix section.

    private static int DEFAULT_LOCAL_PARALLELISM = 1;

    /*
     * Story starts with calculating Timespent for each NewsFeed by
     * Section,Subsection. 1. Map. In the mapper TimeSpent is calculated.
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
     * Feed. 2. Project This can be applied only on Tuples,printing
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
                .<Tuple2<String, String>> project(1, 2).distinct();
        result.print();
    }

    /*
     * Illustration of filter, let us say if the news agency wants to see the
     * section,subsection that has as a reading rate of more than 5 second
     */
    @SuppressWarnings("serial")
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
     * another way of doing map 4. Map Partition.
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
     * // 5. Flat Map -- Do we need to discuss? as it might be discussed in
     * chapter 2 //
     */

    /*
     * now if the user agency wants to calculate the entire time spent by
     * section,subsection then reduce is used by summing up the timespent. 6.
     * Reduce - Only discuss Grouped Reduce in the book. Discuss
     * Shuffle/Partition at this point.
     */
    public static void usingReduce() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple3<String, String, Long>> result = newsFeeds
                .map(new NewsFeedMapper())
                .<Tuple3<String, String, Long>> project(1, 2, 4).groupBy(0, 1)
                .reduce(new ComputeTimeSpentPerSectionAndSubSection());
        result.print();
    }

    /*
     * another way of doing reduce as illustrated above using Aggregegation.
     * 6.1. Reduce by introducing aggregations, same as Above
     */
    public static void usingAggregation() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());        
        DataSet<Tuple3<String,String,Long>> result = newsFeeds.map(new NewsFeedMapper())
                                                              .<Tuple3<String,String,Long>> project(1, 2, 4)
                                                              .groupBy(0, 1)
                                                              .aggregate(Aggregations.SUM, 2);
        result.print();

    }
    
    public static void usingMultipleAggregations() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());        
        DataSet<Tuple5<String,String,Long,Long,Long>> result = newsFeeds.map(new NewsFeedMapper2())                                                              
                                                              .groupBy(0, 1)
                                                              .aggregate(Aggregations.MIN, 2)
                                                              .and(Aggregations.MAX, 3)
                                                              .and(Aggregations.SUM, 4);
        result.print();
    }

    /*
     * another way of doing reduce as illustrated above using some util
     * functions. 6.2. Reduce by using util method sum, same as Above
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
     * Introducing groupBy with KeySelector. 7. using keyselector, another way
     * of selecting keys and using group reduce to have the same result as above
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
     * 8. Group Reduce using Sorting.
     */
    public static void usingGroupReduceSortedKeys() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<Tuple3<String, String, Long>> result = newsFeeds
                .map(new NewsFeedMapper())
                .<Tuple3<String, String, Long>> project(1, 2, 4)
                .groupBy(0)
                .sortGroup(1, Order.DESCENDING)
                .reduceGroup(
                        new SortedGroupReduceComputeTimeSpentBySectionAndSubSection());
        result.print();
    }

    /*
     * 9. using keyselector, another way of selecting keys,sort key and using
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
     * 10. Group Combine - Role of Combine (Point to MapReduce article)
     */
    public static void usingGroupCombine() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeedValues = execEnv
                .fromCollection(NewsFeedParser.parseData());
        DataSet<NewsFeed> newsFeeds = newsFeedValues
               .map(new DomainObjectBasedNewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> combined = newsFeeds
                .groupBy(0, 1)
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
     * 11. basic Join i.e default join which provides Tuple2 with newsfeed and
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
                .map(new DomainObjectBasedPageInformationParser());
        DataSet<Tuple2<NewsFeed, Page>> joinDataSet = newsFeeds
                .join(pageInformationDataSet).where("pageId").equalTo("id");
        joinDataSet.print();
    }

    /*
     * 11.1 basic Join with a Tuple and an Object
     */

    public static void usingBasicJoinWithTupleAndObject() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<String> pageInformationValues = execEnv
                .fromCollection(PageParser.parseData());
        DataSet<Page> pageInformationDataSet = pageInformationValues
                .map(new DomainObjectBasedPageInformationParser());
        DataSet<Tuple5<Long,String, String, String, Long>> tuple5DataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple2<Tuple5<Long, String, String, String, Long>, Page>> joinDataSet = 
                tuple5DataSet.join(pageInformationDataSet)
                             .where("f4").equalTo("id");
        joinDataSet.print();
    }

    /*
     * 11.2 basic Join with a Tuple and an Tuple
     */

    public static void usingBasicJoinWithTupleAndTuple() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<String> pageInformationDataSet = execEnv
                .fromCollection(PageParser.parseData());
        DataSet<Tuple5<Long,String, String, String, Long>> tuple5DataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> tuple3DataSet = pageInformationDataSet
                .map(new PageMapper());
        DataSet<Tuple2<Tuple5<Long,String, String, String, Long>,Tuple3<String, String, Long>>> joinDataSet = 
                tuple5DataSet.join(tuple3DataSet).where("f4").equalTo("f2");
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
        DataSet<Tuple5<Long,String, String, String, Long>> tuple5DataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> tuple3DataSet = pageInformationDataSet
                .map(new PageMapper());
        DataSet<Tuple3<String, String,Long>> joinDataSet = tuple5DataSet.join(tuple3DataSet)
                                                                        .where("4").equalTo("2")
                                                                        .projectFirst(2, 3).projectSecond(0);
        joinDataSet.print();
    }

    /*
     * 13.1 join with hint. Generally the news feed is basically a large table,
     * on the other hand pageInformation is small
     */
    public static void usingJoinWithHint() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());
        DataSet<String> pageInformationDataSet = execEnv
                .fromCollection(PageParser.parseData());
        DataSet<Tuple5<Long,String, String, String, Long>> largeDataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> smallDataSet = pageInformationDataSet
                .map(new PageMapper());
        largeDataSet
                .join(smallDataSet,
                        JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                .where("4").equalTo("2").print();
        smallDataSet
                .join(largeDataSet,
                        JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
                .where("2").equalTo("4").print();
    }

    /*
     * 13.2 join with hint. Generally the news feed is basically a large table,
     * on the other hand pageInformation is small
     */

    public static void usingJoinWithHintUtility() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
                .parseData());

        DataSet<String> pageInformationDataSet = execEnv
                .fromCollection(PageParser.parseData());

        DataSet<Tuple5<Long,String, String, String, Long>> largeDataSet = newsFeeds
                .map(new NewsFeedMapper());
        DataSet<Tuple3<String, String, Long>> smallDataSet = pageInformationDataSet
                .map(new PageMapper());
        largeDataSet.joinWithTiny(smallDataSet).where("4").equalTo("2").print();
        smallDataSet.joinWithHuge(largeDataSet).where("2").equalTo("4").print();
    }

    // After the above code talk about all the Join Algorithms
    // https://ci.apache.org/projects/flink/flink-docs-master/apis/batch/dataset_transformations.html#join-algorithm-hints

    public static void main(String[] args) throws Exception {
        MediaBatchTansformations.usingMap();
        MediaBatchTansformations.usingProject();
        MediaBatchTansformations.usingFilter();
        MediaBatchTansformations.usingMapPartition();
        MediaBatchTansformations.usingReduce();
        MediaBatchTansformations.usingAggregation();
        MediaBatchTansformations.usingSum();
        MediaBatchTansformations.usingGroupReduceSortedKeys();
        MediaBatchTansformations.usingKeySelectorAndGroupReduce();
        MediaBatchTansformations.usingGroupReduceSortedKeysWithKeySelector();
        MediaBatchTansformations.usingGroupCombine();
        MediaBatchTansformations.usingBasicJoin();
        MediaBatchTansformations.usingBasicJoinWithTupleAndObject();
        MediaBatchTansformations.usingBasicJoinWithTupleAndTuple();
        MediaBatchTansformations.usingJoinWithProjection();
        MediaBatchTansformations.usingJoinWithHint();
        MediaBatchTansformations.usingJoinWithHintUtility();
    }
}

/**
 * https://ci.apache.org/projects/flink/flink-docs-master/apis/batch/index.html#
 * iteration-operators-Chapter 5
 * https://ci.apache.org/projects/flink/flink-docs-
 * master/apis/batch/index.html#semantic-annotations
 */
