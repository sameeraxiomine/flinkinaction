package com.manning.fia.c06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class VariableClosureExample {
	public static class RulesAndEventsMapFunction
	      implements FlatMapFunction<Tuple2<String, Integer>, 
	                               Tuple3<String, Integer, String>> {
		private Integer temperatureThreshold = null;
		private Integer pressureThreshold = null;
		
		public RulesAndEventsMapFunction(int temperatureThreshold,int pressureThreshold){
			this.temperatureThreshold = temperatureThreshold;
			this.pressureThreshold = pressureThreshold;
		}

	
		@Override
		public void flatMap(Tuple2<String, Integer> event, Collector<Tuple3<String, Integer, String>> out)
		      throws Exception {
			int threshold = event.f0.equals("temperature")?temperatureThreshold:pressureThreshold;
			if (event.f1 < threshold) {
				out.collect(Tuple3.of(event.f0, event.f1, "ALERT"));
			} else {
				out.collect(Tuple3.of(event.f0, event.f1, "NORMAL"));					
			}
		}
	}

	public static void main(String[] args) throws Exception{
      StreamExecutionEnvironment execEnv =
            StreamExecutionEnvironment.createLocalEnvironment(5);
      execEnv.setParallelism(5);      
      DataStream<Tuple2<String,Integer>> eventSource = execEnv.addSource(new ContinousEventsSource());
      SplitStream<Tuple3<String, Integer, String>> splitStream = 
      		eventSource.flatMap(new RulesAndEventsMapFunction(45,450)).split(new MyOutputSelector());
      DataStream<Tuple3<String, Integer, String> > normal = splitStream.select("NORMAL");
      DataStream<Tuple3<String, Integer, String> > alerts = splitStream.select("ALERT");
      alerts.printToErr();
      normal.print();
		execEnv.execute();
	}
}
