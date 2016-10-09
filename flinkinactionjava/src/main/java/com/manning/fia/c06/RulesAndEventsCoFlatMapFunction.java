package com.manning.fia.c06;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class RulesAndEventsCoFlatMapFunction
      implements CoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, String>> {
	private Map<String, Integer> thresholdByType = new HashMap<>();

	@Override
	public void flatMap1(Tuple2<String, Integer> event, Collector<Tuple3<String, Integer, String>> out)
	      throws Exception {
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
	public void flatMap2(Tuple2<String, Integer> rule, Collector<Tuple3<String, Integer, String>> out) throws Exception {
		thresholdByType.put(rule.f0, rule.f1);
	}
}