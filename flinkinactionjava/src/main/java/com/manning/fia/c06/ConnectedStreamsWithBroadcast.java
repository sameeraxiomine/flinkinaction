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

public class ConnectedStreamsWithBroadcast {
	public static class RulesCoMapFunction
	      implements CoFlatMapFunction<Tuple2<String, Integer>, 
	                               Tuple2<String, Integer>, 
	                               Tuple3<String, Integer, String>> {
		private Map<String, Integer> thresholdByType = new HashMap<>();
		@Override
		public void flatMap1(Tuple2<String, Integer> event,
				Collector<Tuple3<String, Integer, String>> out) throws Exception {
			Integer threshold = this.thresholdByType.get(event.f0);
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
			thresholdByType.put(rule.f0, rule.f1);		
		}
	}

	public static void main(String[] args) throws Exception{
      StreamExecutionEnvironment execEnv =
            StreamExecutionEnvironment.createLocalEnvironment(5);
      execEnv.setParallelism(5);      
      DataStream<Tuple2<String,Integer>> rulesSource = execEnv.addSource(new RulesSource());      
      DataStream<Tuple2<String,Integer>> eventSource = execEnv.addSource(new ContinousEventsSource());
      ConnectedStreams<Tuple2<String, Integer>, Tuple2<String, Integer>> connectedStream = 
      		eventSource.connect(rulesSource.broadcast());      
      SplitStream<Tuple3<String, Integer, String>> splitStream = 
      		connectedStream.flatMap(new RulesCoMapFunction()).split(new MyOutputSelector());
      DataStream<Tuple3<String, Integer, String> > normal = splitStream.select("NORMAL");
      DataStream<Tuple3<String, Integer, String> > alerts = splitStream.select("ALERT");      
      alerts.printToErr();
      normal.print();      
		execEnv.execute();
	}
}
