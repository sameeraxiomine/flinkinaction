package com.manning.fia.c06;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

public class VariableClosureExample {
	public static class RulesMapFunction
	      implements FlatMapFunction<Tuple3<String, Integer,Long>, 
	                               Tuple4<String, Integer, String,String>> {
		private Integer temperatureThreshold = null;
		private Integer pressureThreshold = null;
		
		public RulesMapFunction(int temperatureThreshold,int pressureThreshold){
			this.temperatureThreshold = temperatureThreshold;
			this.pressureThreshold = pressureThreshold;
		}
		private String convertDateTimeToString(long millis){
			return DateTimeFormat.forPattern("yyyyMMddHHmmss").print(millis);
		}
	
		@Override
		public void flatMap(Tuple3<String, Integer, Long> event, Collector<Tuple4<String, Integer, String, String>> out)
		      throws Exception {
			int threshold = event.f0.equals("temperature")?temperatureThreshold:pressureThreshold;
			if (event.f1 < threshold) {
				out.collect(Tuple4.of(event.f0, event.f1, "ALERT",convertDateTimeToString(event.f2)));
			} else {
				out.collect(Tuple4.of(event.f0, event.f1, "NORMAL",convertDateTimeToString(event.f2)));					
			}
		}
	}

	public static void main(String[] args) throws Exception{
      StreamExecutionEnvironment execEnv =
            StreamExecutionEnvironment.createLocalEnvironment(5);
      execEnv.setParallelism(5);      
      DataStream<Tuple3<String,Integer,Long>> eventSource = execEnv.addSource(new ContinousEventsSource());
      SplitStream<Tuple4<String, Integer, String, String>> splitStream = 
      		eventSource.flatMap(new RulesMapFunction(45,450)).split(new MyOutputSelector());
      DataStream<Tuple4<String, Integer, String, String> > normal = splitStream.select("NORMAL");
      DataStream<Tuple4<String, Integer, String, String> > alerts = splitStream.select("ALERT");
      alerts.printToErr();
      normal.print();
		execEnv.execute();
	}
}
