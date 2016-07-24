package com.manning.fia.ch09;

import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.table.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MediaBatchTableTransformations {

  private static int DEFAULT_LOCAL_PARALLELISM = 1;

  /**
   * Register DataSet as a Batch Table with a few selected fields.
   */
  private static void registerDataSetAsATable() throws Exception {
    final ExecutionEnvironment execEnv = ExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv = TableEnvironment
        .getTableEnvironment(execEnv);
    final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<Tuple5<Long, String, String, String, Long>> result =
        newsFeeds.map(new NewsFeedMapper());
    tableEnv.registerDataSet("NewsFeed", result);
  }

  /*
   * Illustration of filter, let us say if the news agency wants to see the
   * section,subsection that has as a reading rate of more than 6 second.
   *
   */
  private static void usingFilter() throws Exception {
    final ExecutionEnvironment execEnv = ExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);
    DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
        .parseData());
    DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
        .map(new NewsFeedMapper());
    Table table = tableEnv.fromDataSet(result, "page, section, subsection, topic, timespent");
    Table output = table.select("page, section, timespent").filter("timespent > 6000");

    tableEnv.toDataSet(output, Row.class).print();
  }

  /*
   * Illustration of Where, let us say if the news agency wants to see the
   * section,subsection that has as a reading rate of more than 6 second.
   *
   */
  private static void usingWhereSQL() throws Exception {
    final ExecutionEnvironment execEnv = ExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);
    DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
        .parseData());
    DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
        .map(new NewsFeedMapper());
    Table table = tableEnv.fromDataSet(result, "page, section, subsection, topic, timespent");

    tableEnv.registerTable("NewsFeed", table);
    Table output = tableEnv.sql("SELECT page, section, topic FROM NewsFeed WHERE timespent > 6000");

    tableEnv.toDataSet(output, Row.class).print();
  }

  /*
   * Illustration of GroupBy and fetch records that have
   * a reading rate of more than 6 second.
   *
   */
  private static void usingGroupBy() throws Exception {
    final ExecutionEnvironment execEnv = ExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);
    DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds.map(new NewsFeedMapper());
    Table table = tableEnv.fromDataSet(result, "page, section, subsection, topic, timespent");

    Table output = table.groupBy("page, section, topic")
        .select("page, section, topic, timespent.sum as total_time_spent")
        .filter("total_time_spent > 6000");

    tableEnv.toDataSet(output, Row.class).print();
  }

  /*
   * Illustration of GroupBy using SQL
   *
   */
  private static void usingGroupBySQL() throws Exception {
    final ExecutionEnvironment execEnv =
        ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);
    DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
        .map(new NewsFeedMapper());
    Table table = tableEnv.fromDataSet(result, "page, section, subsection, topic, timespent");
    tableEnv.registerTable("NewsFeed", table);

    Table output = tableEnv.sql("SELECT page, section, topic, " +
        "SUM(timespent) as total_time_spent FROM NewsFeed " +
        "GROUP BY page, section, topic");

    tableEnv.toDataSet(output, Row.class).print();
  }

  /**
   * Illustrates a Join
   *
   */
  private static void usingJoin() throws Exception {
    final ExecutionEnvironment execEnv = ExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);

    DataSet<String> newsFeed1 = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<String> newsFeed2 = execEnv.fromCollection(NewsFeedParser.parseData("/media/pipe/newsfeed2"));

    DataSet<Tuple5<Long, String, String, String, Long>> newsFeedValues1 = newsFeed1.map(new NewsFeedMapper());
    DataSet<Tuple5<Long, String, String, String, Long>> newsFeedValues2 = newsFeed2.map(new NewsFeedMapper());

    Table table1 = tableEnv.fromDataSet(newsFeedValues1, "page, section, subsection, topic, timespent");
    Table table2 = tableEnv.fromDataSet(newsFeedValues2, "page1, section1, subsection1, topic1, timespent1");

    Table output = table1.join(table2).where("page = page1");
    tableEnv.toDataSet(output, Row.class).print();
  }

  /**
   * Illustrates Join using regular SQL
   *
   */
  private static void usingJoinSQL() throws Exception {
    final ExecutionEnvironment execEnv = ExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);

    DataSet<String> newsFeed1 = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<String> newsFeed2 = execEnv.fromCollection(NewsFeedParser.parseData("/media/pipe/newsfeed2"));

    DataSet<Tuple5<Long, String, String, String, Long>> newsFeedValues1 = newsFeed1.map(new NewsFeedMapper());
    DataSet<Tuple5<Long, String, String, String, Long>> newsFeedValues2 = newsFeed2.map(new NewsFeedMapper());

    Table table1 = tableEnv.fromDataSet(newsFeedValues1, "page, section, subsection, topic, timespent");
    Table table2 = tableEnv.fromDataSet(newsFeedValues2, "page, section, subsection, topic, timespent");

    tableEnv.registerTable("NewsFeed1", table1);
    tableEnv.registerTable("NewsFeed2", table2);

    Table output = tableEnv.sql("SELECT * from NewsFeed1, NewsFeed2 WHERE NewsFeed1.page = NewsFeed2.page");
    tableEnv.toDataSet(output, Row.class).print();
  }

  /**
   * Illustrates a Union, does a union over similar types of columns
   *
   */
  private static void usingUnion() throws Exception {
    final ExecutionEnvironment execEnv = ExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);

    DataSet<String> newsFeed1 = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<String> newsFeed2 = execEnv.fromCollection(NewsFeedParser.parseData("/media/pipe/newsfeed2"));

    DataSet<Tuple5<Long, String, String, String, Long>> newsFeedValues1 = newsFeed1.map(new NewsFeedMapper());
    DataSet<Tuple5<Long, String, String, String, Long>> newsFeedValues2 = newsFeed2.map(new NewsFeedMapper());

    Table table1 = tableEnv.fromDataSet(newsFeedValues1, "page, section, subsection, topic, timespent");
    Table table2 = tableEnv.fromDataSet(newsFeedValues2, "page1, section1, subsection1, topic1, timespent1");

    Table output = table1.union(table2);
    tableEnv.toDataSet(output, Row.class).print();
  }

  /**
   * Illustrates a Union using regular SQL, does a union over similar types of columns
   *
   */
  private static void usingUnionAllSQL() throws Exception {
    final ExecutionEnvironment execEnv = ExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);

    DataSet<String> newsFeed1 = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<String> newsFeed2 = execEnv.fromCollection(NewsFeedParser.parseData("/media/pipe/newsfeed2"));

    DataSet<Tuple5<Long, String, String, String, Long>> newsFeedValues1 = newsFeed1.map(new NewsFeedMapper());
    DataSet<Tuple5<Long, String, String, String, Long>> newsFeedValues2 = newsFeed2.map(new NewsFeedMapper());

    Table table1 = tableEnv.fromDataSet(newsFeedValues1, "page, section, subsection, topic, timespent");
    Table table2 = tableEnv.fromDataSet(newsFeedValues2, "page, section, subsection, topic, timespent");

    tableEnv.registerTable("NewsFeed1", table1);
    tableEnv.registerTable("NewsFeed2", table2);

    Table output = tableEnv.sql("SELECT * from NewsFeed1 UNION ALL SELECT * from NewsFeed2");
    tableEnv.toDataSet(output, Row.class).print();
  }

  /**
   * Illustrates an OrderBy using SQL
   *
   */
  private static void usingOrderBySQL() throws Exception {
    final ExecutionEnvironment execEnv =
        ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);
    DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
        .map(new NewsFeedMapper());
    Table table = tableEnv.fromDataSet(result, "page, section, subsection, topic, timespent");
    tableEnv.registerTable("NewsFeed", table);

    Table output = tableEnv.sql(
        "SELECT page, section, topic FROM NewsFeed ORDER BY topic");

    tableEnv.toDataSet(output, Row.class).print();

  }

  /**
   * Illustrates an OrderBy SQL
   *
   */
  private static void usingOrderBy() throws Exception {
    final ExecutionEnvironment execEnv =
        ExecutionEnvironment.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final BatchTableEnvironment tableEnv =
        BatchTableEnvironment.getTableEnvironment(execEnv);
    DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
    DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
        .map(new NewsFeedMapper());
    Table table = tableEnv.fromDataSet(result, "page, section, subsection, topic, timespent");
    tableEnv.registerTable("NewsFeed", table);

    Table output = table.select("page, section, topic").orderBy("topic");

    tableEnv.toDataSet(output, Row.class).print();
  }


  // Streaming SQL, presently supports SELECT, WHERE, FROM and UNION clauses

  /**
   * Register DataStream as a Stream Table with a few selected fields.
   */
  private static void registerDataStreamAsTable() throws Exception {
    final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final StreamTableEnvironment tableEnv = TableEnvironment
        .getTableEnvironment(execEnv);
    final DataStream<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
    DataStream<Tuple5<Long, String, String, String, Long>> result =
        newsFeeds.map(new NewsFeedMapper());
    tableEnv.registerDataStream("NewsFeed", result);
  }

  /*
   * Illustration of filter, let us say if the news agency wants to see the
   * section,subsection that has as a reading rate of more than 6 second.
   *
   */
  private static void usingStreamFilter() throws Exception {
    final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final StreamTableEnvironment tableEnv =
        StreamTableEnvironment.getTableEnvironment(execEnv);
    DataStream<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
        .parseData());
    DataStream<Tuple5<Long, String, String, String, Long>> result = newsFeeds
        .map(new NewsFeedMapper());
    Table table = tableEnv.fromDataStream(result, "page, section, subsection, topic, timespent");
    Table output = table.select("page, section, timespent").filter("timespent > 6000");

    tableEnv.toDataStream(output, Row.class).print();
  }

  /*
   * Illustration of Where, let us say if the news agency wants to see the
   * section,subsection that has as a reading rate of more than 6 second.
   *
   */
  private static void usingStreamWhereSQL() throws Exception {
    final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final StreamTableEnvironment tableEnv =
        StreamTableEnvironment.getTableEnvironment(execEnv);
    DataStream<String> newsFeeds = execEnv.fromCollection(NewsFeedParser
        .parseData());
    DataStream<Tuple5<Long, String, String, String, Long>> result = newsFeeds
        .map(new NewsFeedMapper());
    Table table = tableEnv.fromDataStream(result, "page, section, subsection, topic, timespent");

    tableEnv.registerTable("NewsFeed", table);
    Table output = tableEnv.sql("SELECT STREAM page, section, topic FROM NewsFeed WHERE timespent > 6000");

    tableEnv.toDataStream(output, Row.class).print();
  }

  /**
   * Illustrates a Union using regular SQL, does a union over similar types of columns
   *
   */
  private static void usingStreamUnionAllSQL() throws Exception {
    final StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
        .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
    final StreamTableEnvironment tableEnv =
        StreamTableEnvironment.getTableEnvironment(execEnv);

    DataStream<String> newsFeed1 = execEnv.fromCollection(NewsFeedParser.parseData());
    DataStream<String> newsFeed2 = execEnv.fromCollection(NewsFeedParser.parseData("/media/pipe/newsfeed2"));

    DataStream<Tuple5<Long, String, String, String, Long>> newsFeedValues1 = newsFeed1.map(new NewsFeedMapper());
    DataStream<Tuple5<Long, String, String, String, Long>> newsFeedValues2 = newsFeed2.map(new NewsFeedMapper());

    Table table1 = tableEnv.fromDataStream(newsFeedValues1, "page, section, subsection, topic, timespent");
    Table table2 = tableEnv.fromDataStream(newsFeedValues2, "page, section, subsection, topic, timespent");

    tableEnv.registerTable("NewsFeed1", table1);
    tableEnv.registerTable("NewsFeed2", table2);

    Table output = tableEnv.sql("SELECT STREAM * from NewsFeed1 UNION ALL SELECT STREAM * from NewsFeed2");
    tableEnv.toDataStream(output, Row.class).print();
  }

  public static void main(String[] args) throws Exception {
    MediaBatchTableTransformations.registerDataSetAsATable();
    MediaBatchTableTransformations.usingFilter();
    MediaBatchTableTransformations.usingWhereSQL();
    MediaBatchTableTransformations.usingGroupBy();
    MediaBatchTableTransformations.usingGroupBySQL();
    MediaBatchTableTransformations.usingJoin();
    MediaBatchTableTransformations.usingJoinSQL();
    MediaBatchTableTransformations.usingUnion();
    MediaBatchTableTransformations.usingUnionAllSQL();
    MediaBatchTableTransformations.usingOrderBySQL();
    MediaBatchTableTransformations.usingOrderBy();
    MediaBatchTableTransformations.registerDataStreamAsTable();
    MediaBatchTableTransformations.usingStreamFilter();
    MediaBatchTableTransformations.usingStreamWhereSQL();
    MediaBatchTableTransformations.usingStreamUnionAllSQL();
  }

}
