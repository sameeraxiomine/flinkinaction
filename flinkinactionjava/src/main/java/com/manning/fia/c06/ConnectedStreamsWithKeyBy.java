package com.manning.fia.c06;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

public class ConnectedStreamsWithKeyBy {
	public static class RulesCoMapFunction
	      implements CoFlatMapFunction<Tuple3<String, Integer,Long>, 
	                               Tuple3<String, Integer,Long>, 
	                               Tuple4<String, Integer, String,String>> {
		Integer threshold = null;
		private String convertDateTimeToString(long millis){
			return DateTimeFormat.forPattern("yyyyMMddHHmmss").print(millis);
		}
		@Override
		public void flatMap1(Tuple3<String, Integer,Long> event,
				Collector<Tuple4<String, Integer, String, String>> out) throws Exception {			
			if (threshold != null) {
				if (event.f1 < threshold) {
					out.collect(Tuple4.of(event.f0, event.f1, "ALERT, Threshold="+Integer.toString(threshold),convertDateTimeToString(event.f2)));
				} else {
					out.collect(Tuple4.of(event.f0, event.f1, "NORMAL, Threshold="+Integer.toString(threshold),convertDateTimeToString(event.f2)));					
				}
			} else {
				out.collect(Tuple4.of(event.f0, event.f1, "NoRuleFound",convertDateTimeToString(event.f2)));				
			}
			
		}

		@Override
		public void flatMap2(Tuple3<String, Integer,Long> rule,Collector<Tuple4<String, Integer, String, String>> out) throws Exception {					
			threshold = rule.f1;
		}
	}

	public static void main(String[] args) throws Exception{
      StreamExecutionEnvironment execEnv =
            StreamExecutionEnvironment.createLocalEnvironment(5);
      execEnv.setParallelism(5);      
      DataStream<Tuple3<String,Integer,Long>> rulesSource = execEnv.addSource(new RulesSource());      
      DataStream<Tuple3<String,Integer,Long>> eventSource = execEnv.addSource(new EventsSource());
      ConnectedStreams<Tuple3<String, Integer,Long>, Tuple3<String, Integer,Long>> connectedStream = 
      		eventSource.connect(rulesSource).keyBy(0, 0);      
		connectedStream.flatMap(new RulesCoMapFunction()).printToErr();
		execEnv.execute();
	}
}
