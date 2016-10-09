package com.manning.fia.c06;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;

import com.manning.fia.c06.ConnectedStreamsWithBroadcast.RulesCoMapFunction;

public class ConnectedStreamsWithKeyBy {
	public static class RulesCoMapFunction
	      implements CoFlatMapFunction<Tuple2<String, Integer>, 
	                               Tuple2<String, Integer>, 
	                               Tuple3<String, Integer, String>> {
		Integer threshold = null;
		private String convertDateTimeToString(long millis){
			return DateTimeFormat.forPattern("yyyyMMddHHmmss").print(millis);
		}
		@Override
		public void flatMap1(Tuple2<String, Integer> event,
				Collector<Tuple3<String, Integer, String>> out) throws Exception {			
			if (threshold != null) {
				if (event.f1 < threshold) {
					out.collect(Tuple3.of(event.f0, event.f1, "ALERT"));
				} else {
					out.collect(Tuple3.of(event.f0, event.f1, "NORMAL"));					
				}
			} else {
				out.collect(Tuple3.of(event.f0, event.f1, "NORULE"));				
			}
			
		}

		@Override
		public void flatMap2(Tuple2<String, Integer> rule,Collector<Tuple3<String, Integer, String>> out) throws Exception {					
			threshold = rule.f1;
		}
	}

	public static void main(String[] args) throws Exception{
      StreamExecutionEnvironment execEnv =
            StreamExecutionEnvironment.createLocalEnvironment(5);
      execEnv.setParallelism(5);      
      DataStream<Tuple2<String,Integer>> rulesSource = execEnv.addSource(new RulesSource());      
      DataStream<Tuple2<String,Integer>> eventSource = execEnv.addSource(new ContinousEventsSource());
      ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connectedStream = 
      		eventSource.connect(rulesSource).keyBy(0, 0);      
		
      SplitStream<Tuple3<String, Integer, String>> splitStream = 
      		connectedStream.flatMap(new RulesCoMapFunction()).split(new MyOutputSelector());
      DataStream<Tuple3<String, Integer, String> > normal = splitStream.select("NORMAL");
      DataStream<Tuple3<String, Integer, String> > alerts = splitStream.select("ALERT");
      DataStream<Tuple3<String, Integer, String> > normalButNoRules = splitStream.select("NORULE");
      alerts.printToErr();
      normal.print();
      normalButNoRules.print();
		execEnv.execute();
	}
}
