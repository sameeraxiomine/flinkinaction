package com.manning.fia.ch09;

import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedParser;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.BatchTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;
import org.apache.flink.api.table.TableEnvironment;

public class MediaBatchTableTransformations {

  private static int DEFAULT_LOCAL_PARALLELISM = 1;

  /**
	 * Register DataSet as a Batch Table with a few selected fields.
	 */
	 public static void registerDataSetAsATable() throws Exception {
	   final ExecutionEnvironment execEnv = ExecutionEnvironment
			 .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
	   final BatchTableEnvironment tableEnv = TableEnvironment
			 .getTableEnvironment(execEnv);
	   final DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
		 DataSet<Tuple5<Long, String, String, String, Long>> result =
			 newsFeeds.map(new NewsFeedMapper());
		 Table table = tableEnv.fromDataSet(result, "page, section, subsection, topic, timespent");
	   tableEnv.registerTable("NewsFeed", table);
		 tableEnv.toDataSet(table, Row.class).print();
	 }

 /*
	* Illustration of filter, let us say if the news agency wants to see the
	* section,subsection that has as a reading rate of more than 6 second.
	*
	*/
 public static void usingFilter() throws Exception {
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
	public static void usingWhereSQL() throws Exception {
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
	public static void usingGroupBy() throws Exception {
		final ExecutionEnvironment execEnv = ExecutionEnvironment
		.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
		final BatchTableEnvironment tableEnv =
		BatchTableEnvironment.getTableEnvironment(execEnv);
		DataSet<String> newsFeeds = execEnv.fromCollection(NewsFeedParser.parseData());
		DataSet<Tuple5<Long, String, String, String, Long>> result = newsFeeds
		.map(new NewsFeedMapper());
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
	public static void usingGroupBySQL() throws Exception {
		final ExecutionEnvironment execEnv = ExecutionEnvironment
		.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
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



 public static void main(String[] args) throws Exception {
	 MediaBatchTableTransformations.registerDataSetAsATable();
	 MediaBatchTableTransformations.usingFilter();
	 MediaBatchTableTransformations.usingWhereSQL();
	 MediaBatchTableTransformations.usingGroupBy();
	 MediaBatchTableTransformations.usingGroupBySQL();
 }

}
