package com.manning.fia.c06;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class RichRulesAndEventsMapFunction
      extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, String>> {

	private Integer temperatureThreshold = null;
	private Integer pressureThreshold = null;

	private IntCounter eventsCounter = new IntCounter();
	private IntCounter alarmCounter = new IntCounter();

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		getRuntimeContext().addAccumulator("eventCounter", eventsCounter);
		getRuntimeContext().addAccumulator("alarmCounter", alarmCounter);
	}

	public RichRulesAndEventsMapFunction(int temperatureThreshold, int pressureThreshold) {
		this.temperatureThreshold = temperatureThreshold;
		this.pressureThreshold = pressureThreshold;
	}

	@Override
	public void flatMap(Tuple2<String, Integer> event, Collector<Tuple3<String, Integer, String>> out) throws Exception {
		eventsCounter.add(1);
		int threshold = event.f0.equals("temperature") ? temperatureThreshold : pressureThreshold;
		if (event.f1 < threshold) {
			alarmCounter.add(1);
			out.collect(Tuple3.of(event.f0, event.f1, "ALERT"));
		} else {
			out.collect(Tuple3.of(event.f0, event.f1, "NORMAL"));
		}
	}

}