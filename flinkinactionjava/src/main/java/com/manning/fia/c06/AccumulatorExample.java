package com.manning.fia.c06;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class AccumulatorExample {

	public static void main(String[] args) throws Exception {
      StreamExecutionEnvironment execEnv =
            StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple2<String, Integer>> eventSource = execEnv.addSource(new ContinousEventsSource());
		SplitStream<Tuple3<String, Integer, String>> splitStream = eventSource.flatMap(new RichRulesAndEventsMapFunction(45, 450))
		                                                                      .split(new MyOutputSelector());
		DataStream<Tuple3<String, Integer, String>> normal = splitStream.select("NORMAL");
		DataStream<Tuple3<String, Integer, String>> alerts = splitStream.select("ALERT");
		alerts.printToErr();
		normal.print();
		execEnv.execute();

	}

}