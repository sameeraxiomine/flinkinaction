package com.manning.fia.ch09;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.model.media.Page;
import com.manning.fia.transformations.media.DomainObjectBasedNewsFeedMapper;
import com.manning.fia.transformations.media.DomainObjectBasedPageMapper;
import com.manning.fia.transformations.media.NewsFeedMapper;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.transformations.media.PageParser;
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
	   tableEnv.registerDataSet("NewsFeed", result);
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
	public static void usingGroupBySQL() throws Exception {
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

	public static void usingJoin() throws Exception {
     final ExecutionEnvironment execEnv = ExecutionEnvironment
         .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
     final BatchTableEnvironment tableEnv =
       BatchTableEnvironment.getTableEnvironment(execEnv);

     DataSet<String> newsFeedValues = execEnv.fromCollection(NewsFeedParser.parseData());
     DataSet<String> pageInformationValues = execEnv.fromCollection(PageParser.parseData());
     DataSet<NewsFeed> newsFeeds = newsFeedValues.map(new DomainObjectBasedNewsFeedMapper());
     DataSet<Page> pagesInfo = pageInformationValues.map(new DomainObjectBasedPageMapper());

     tableEnv.registerDataSet("NewsFeed", newsFeeds);
		 tableEnv.registerDataSet("Pages", pagesInfo);

		 Table newsTable = tableEnv.scan("NewsFeed");
		 Table pagesTable = tableEnv.scan("Pages");

     Table output = newsTable.join(pagesTable).where("id === pageId");
     tableEnv.toDataSet(output, Row.class).print();
  }

	public static void usingUnion() throws Exception {
		final ExecutionEnvironment execEnv = ExecutionEnvironment
		.createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
		final BatchTableEnvironment tableEnv =
		BatchTableEnvironment.getTableEnvironment(execEnv);

		DataSet<String> newsFeedValues = execEnv.fromCollection(NewsFeedParser.parseData());
		DataSet<String> pageInformationValues = execEnv.fromCollection(PageParser.parseData());
		DataSet<NewsFeed> newsFeeds = newsFeedValues.map(new DomainObjectBasedNewsFeedMapper());
		DataSet<Page> pagesInfo = pageInformationValues.map(new DomainObjectBasedPageMapper());

		tableEnv.registerDataSet("NewsFeed", newsFeeds);
		tableEnv.registerDataSet("Pages", pagesInfo);

		Table newsTable = tableEnv.scan("NewsFeed");
		Table pagesTable = tableEnv.scan("Pages");

		Table output = newsTable.union(pagesTable);
		tableEnv.toDataSet(output, Row.class).print();
	}

//	public static void usingXY() throws Exception {
//		List<Tuple3<Long, String, Integer>> input = new ArrayList<>();
//		input.add(new Tuple3<>(3L,"test",1));
//		input.add(new Tuple3<>(5L,"test2",2));
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
//		DataStream<Tuple3<Long, String, Integer>> ds = env.fromCollection(input);
//
//		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
//
//		tableEnv.registerDataStream("Words", ds, "frequency, word, pos");
//		// run a SQL query on the Table and retrieve the result as a new Table
//		Table result = tableEnv.sql(
//		"SELECT STREAM word, pos FROM Words WHERE frequency > 2");
//
//		tableEnv.toDataStream(result, Row.class).writeAsText("home/smarthi/tmp");
//
//	}

	public static void usingOrderBySQL() throws Exception {
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

	public static void usingOrderBy() throws Exception {
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

 public static void main(String[] args) throws Exception {
//	 MediaBatchTableTransformations.registerDataSetAsATable();
//	 MediaBatchTableTransformations.usingFilter();
//	 MediaBatchTableTransformations.usingWhereSQL();
//	 MediaBatchTableTransformations.usingGroupBy();
//	 MediaBatchTableTransformations.usingGroupBySQL();
//   MediaBatchTableTransformations.usingUnion();
//	 MediaBatchTableTransformations.usingXY();
	 MediaBatchTableTransformations.usingOrderBySQL();
	 MediaBatchTableTransformations.usingOrderBy();

 }

}
