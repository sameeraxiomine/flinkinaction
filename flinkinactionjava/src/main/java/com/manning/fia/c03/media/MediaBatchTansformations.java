package com.manning.fia.c03.media;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.model.media.PageInformation;
import com.manning.fia.transformations.media.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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

public class MediaBatchTansformations {

    // We can talk about the way we can run our program in local mode and remote mode in appendix section.

    private static int DEFAULT_LOCAL_PARALLELISM = 1;

    /* Story starts with calculating Timespent for each NewsFeed by Section,Subsection.
    * 1. Map.
    * In the mapper TimeSpent is calculated.
    *
    */
    public static void usingMap() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        newsFeeds.map(new MapTokenizeNewsFeed()).print();
    }


    /* One can also project what the user needs from Mapper, let us say if the application also wants distinct
    section,subsection along the entire news Feed.
    * 2. Project
    * This can be applied only on Tuples,printing section,subection distinct values
    */
    public static void usingProject() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        newsFeeds.map(new MapTokenizeNewsFeed()).project(0, 1).distinct().print();

    }

    /* Illustration of filter, let us say if the news agency wants to see the section,subsection that has as a
    reading rate of more than 5 sec.
        * 3
        */
    public static void usingFilter() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        newsFeeds.map(new MapTokenizeNewsFeed()).filter(new FilterFunction<Tuple5<String, String, String, Long, Long>>
                () {
            @Override
            public boolean filter(Tuple5<String, String, String, Long, Long> tuple5) throws Exception {
                return tuple5.f3 > 5;
            }
        }).print();
    }

    /* another way of doing map
    * 4. Map Partition.
    */
    public static void usingMapPartition() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        newsFeeds.mapPartition(new MapPartitionTokenizeAndComputeTransactionValue()).print();

    }

    /*
//        5. Flat Map -- Do we need to discuss? as it might be discussed in chapter 2
//


    */

    /* now if the user agency wants to calculate the entire time spent by section,subsection then reduce is used by
    summing up the timespent.
   * 6. Reduce - Only discuss Grouped Reduce in the book. Discuss
   * Shuffle/Partition at this point.
   */
    public static void usingReduce() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        newsFeeds.map(new MapTokenizeNewsFeed())
                .<Tuple3<String, String, Long>>project(0, 1, 3)
                .groupBy(0, 1).reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> reduce(
                    Tuple3<String, String, Long> value1,
                    Tuple3<String, String, Long> value2) throws Exception {
                return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
            }
        }).print();
    }


    /* another way of doing reduce as illustrated above using Aggregegation.
    6.1. Reduce by introducing aggregations, same as Above
    */
    public static void usingAggregation() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        newsFeeds.map(new MapTokenizeNewsFeed()).project(0, 1, 3).groupBy(0, 1).aggregate(Aggregations.SUM, 2).print();

    }

    /*another way of doing reduce as illustrated above using some util functions.
    * 6.2. Reduce by using util method sum, same as Above
    */
    public static void usingSum() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        newsFeeds.map(new MapTokenizeNewsFeed()).project(0, 1, 3).groupBy(0, 1).sum(2).print();

    }

    /*  Introducing groupBy with KeySelector.
        7. using keyselector, another way of selecting keys
       and using group reduce to have the same result as above
     */

    public static void usingKeySelectorAndGroupReduce() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeedValues = execEnv.fromCollection(NewsFeedParser.parseData());
        final DataSet<NewsFeed> newsFeeds=newsFeedValues.map(new DomainObjectBasedNewsFeedParser());

        newsFeeds.groupBy(new KeySelector<NewsFeed,
                Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(NewsFeed value) throws Exception {
                return new Tuple2<>(value.getSection(), value.getSubSection());
            }
        }).reduceGroup(new GroupReduceFunction<NewsFeed, Tuple3<String, String, Long>>() {
            @Override
            public void reduce(Iterable<NewsFeed> newsFeeds, Collector<Tuple3<String, String, Long>> out) throws
                    Exception {
                long total = 0l;
                String section = null;
                String subSection = null;
                for (NewsFeed feed : newsFeeds) {
                    section = feed.getSection();
                    subSection = feed.getSubSection();
                    long timeSpent = feed.getEndTimeStamp() - feed.getStartTimeStamp();
                    total = total + timeSpent;
                }
                out.collect(new Tuple3<>(section, subSection, total));
            }
        }).print();

    }

    /*
        8.  Group Reduce using Sorting.
     */
    public static void usingGroupReduceSortedKeys() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        newsFeeds.map(new MapTokenizeNewsFeed())
                .<Tuple3<String, String, Long>>project(0, 1, 3)
                .groupBy(0, 1)
                .sortGroup(0, Order.DESCENDING)
                .reduceGroup(new GroupReduceFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
                    @Override
                    public void reduce(Iterable<Tuple3<String, String, Long>> newsFeeds,
                                       Collector<Tuple3<String, String, Long>> out) throws Exception {
                        long total = 0l;
                        String section = null;
                        String subSection = null;
                        for (Tuple3<String, String, Long> feed : newsFeeds) {
                            section = feed.f0;
                            subSection = feed.f1;
                            long timeSpent = feed.f2;
                            total = total + timeSpent;
                        }
                        out.collect(new Tuple3<>(section, subSection, total));
                    }
                }).print();
    }

    /*
        9. using keyselector, another way of selecting keys,sort key and using group reduce to have the same result
         as above
     */

    public static void usingGroupReduceSortedKeysWithKeySelector() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeedValues = execEnv.fromCollection(NewsFeedParser.parseData());
        final DataSet<NewsFeed> newsFeeds=newsFeedValues.map(new DomainObjectBasedNewsFeedParser());
        newsFeeds
                .groupBy(new KeySelector<NewsFeed, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(NewsFeed value) throws Exception {
                        return new Tuple2<>(value.getSection(), value.getSubSection());
                    }
                })
                .sortGroup(new KeySelector<NewsFeed, String>() {
                    @Override
                    public String getKey(NewsFeed value) throws Exception {
                        return value.getTopic();
                    }
                }, Order.ASCENDING).
                reduceGroup(new GroupReduceFunction<NewsFeed, Tuple3<String, String, Long>>() {
                    @Override
                    public void reduce(Iterable<NewsFeed> newsFeeds, Collector<Tuple3<String, String, Long>> out) throws
                            Exception {
                        long total = 0l;
                        String section = null;
                        String subSection = null;
                        for (NewsFeed feed : newsFeeds) {
                            section = feed.getSection();
                            subSection = feed.getSubSection();
                            final long timeSpent = feed.getEndTimeStamp() - feed.getStartTimeStamp();
                            total = total + timeSpent;
                        }
                        out.collect(new Tuple3<>(section, subSection, total));
                    }
                }).print();

    }


    /*
    10. Group Combine - Role of Combine (Point to MapReduce article)
    */
    public static void usingGroupCombine() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeedValues = execEnv.fromCollection(NewsFeedParser.parseData());
        final DataSet<NewsFeed> newsFeeds=newsFeedValues.map(new DomainObjectBasedNewsFeedParser());
        newsFeeds.groupBy(new KeySelector<NewsFeed,
                Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(NewsFeed value) throws Exception {
                return new Tuple2<>(value.getSection(), value.getSubSection());
            }
        }).combineGroup(new GroupCombineFunction<NewsFeed, Tuple3<String, String, Long>>() {
            @Override
            public void combine(Iterable<NewsFeed> values, Collector<Tuple3<String, String, Long>> out) throws
                    Exception {
                long total = 0l;
                String section = null;
                String subSection = null;
                for (NewsFeed feed : values) {
                    section = feed.getSection();
                    subSection = feed.getSubSection();
                    final long timeSpent = feed.getEndTimeStamp() - feed.getStartTimeStamp();
                    total = total + timeSpent;
                }
                out.collect(new Tuple3<>(section, subSection, total));
            }
        }).print();

    }


    /*
      11. basic Join i.e default join which provides Tuple2 with newsfeed and page
     */

    public static void usingBasicJoin() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeedValues = execEnv.fromCollection(NewsFeedParser.parseData());
        final DataSet<String> pageInformationValues = execEnv.fromCollection(PageInformationParser.parseData());

        final DataSet<NewsFeed> newsFeeds=newsFeedValues.map(new DomainObjectBasedNewsFeedParser());
        final DataSet<PageInformation> pageInformationDataSet=pageInformationValues.map(new DomainObjectBasedPageInformationParser());

        DataSet<Tuple2<NewsFeed, PageInformation>> joinDataSet = newsFeeds.join(pageInformationDataSet).where("pageId")
                .equalTo("id");
        joinDataSet.print();
    }


    /*
      11.1 basic Join with a Tuple and an Object
     */

    public static void usingBasicJoinWithTupleAndObject() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);

        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
        final DataSet<String> pageInformationValues = execEnv.fromCollection(PageInformationParser.parseData());

        final DataSet<PageInformation> pageInformationDataSet=pageInformationValues.map(new DomainObjectBasedPageInformationParser());

        final DataSet<Tuple5<String, String, String, Long, Long>> tuple5DataSet = newsFeeds.map(new MapTokenizeNewsFeed
                ());
        tuple5DataSet.join(pageInformationDataSet).where("f4").equalTo("id").print();
    }

      /*
      11.2 basic Join with a Tuple and an Tuple
     */

    public static void usingBasicJoinWithTupleAndTuple() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());

        final DataSet<String> pageInformationDataSet = execEnv.fromCollection(PageInformationParser.parseData());
        final DataSet<Tuple5<String, String, String, Long, Long>> tuple5DataSet = newsFeeds.map(new MapTokenizeNewsFeed
                ());
        final DataSet<Tuple3<String, String, Long>> tuple3DataSet = pageInformationDataSet.map(new
                MapTokenizePageInformation());
        tuple5DataSet.join(tuple3DataSet).where("f4").equalTo("f2").print();
    }

    /*
     12  join function with projection.
     This will give us the topic,timespent on a topic for each news feed and the page URL.

     */

    public static void usingJoinWithProjection() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());

        final DataSet<String> pageInformationDataSet = execEnv.fromCollection(PageInformationParser.parseData());

        final DataSet<Tuple5<String, String, String, Long, Long>> tuple5DataSet = newsFeeds.map(new MapTokenizeNewsFeed
                ());
        final DataSet<Tuple3<String, String, Long>> tuple3DataSet = pageInformationDataSet.map(new
                MapTokenizePageInformation());
        tuple5DataSet.join(tuple3DataSet).where("4").equalTo("2").projectFirst(2, 3).projectSecond(0).print();
    }

    /*
    13.1 join with hint. Generally the news feed is basically a large table, on the other hand pageInformation is small

     */
    public static void usingJoinWithHint() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());

        final DataSet<String> pageInformationDataSet = execEnv.fromCollection(PageInformationParser.parseData());

        final DataSet<Tuple5<String, String, String, Long, Long>> largeDataSet = newsFeeds.map(new MapTokenizeNewsFeed
                ());
        final DataSet<Tuple3<String, String, Long>> smallDataSet = pageInformationDataSet.map(new
                MapTokenizePageInformation());
        largeDataSet.join(smallDataSet, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND).where("4").equalTo("2").print();
        smallDataSet.join(largeDataSet, JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST).where("2").equalTo("4").print();
    }

    /*
    13.2 join with hint. Generally the news feed is basically a large table, on the other hand pageInformation is small

     */

    public static void usingJoinWithHintUtility() throws Exception {
        final ExecutionEnvironment execEnv = ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());

        final DataSet<String> pageInformationDataSet = execEnv.fromCollection(PageInformationParser.parseData());

        final DataSet<Tuple5<String, String, String, Long, Long>> largeDataSet = newsFeeds.map(new MapTokenizeNewsFeed
                ());
        final DataSet<Tuple3<String, String, Long>> smallDataSet = pageInformationDataSet.map(new
                MapTokenizePageInformation());
        largeDataSet.joinWithTiny(smallDataSet).where("4").equalTo("2").print();
        smallDataSet.joinWithHuge(largeDataSet).where("2").equalTo("4").print();
    }

    //After the above code talk about all the Join Algorithms https://ci.apache.org/projects/flink/flink-docs-master/apis/batch/dataset_transformations.html#join-algorithm-hints

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
 * https://ci.apache.org/projects/flink/flink-docs-master/apis/batch/index.html#iteration-operators-Chapter 5
 *https://ci.apache.org/projects/flink/flink-docs-master/apis/batch/index.html#semantic-annotations
 */
