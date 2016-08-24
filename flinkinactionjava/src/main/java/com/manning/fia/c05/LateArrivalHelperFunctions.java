package com.manning.fia.c05;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class LateArrivalHelperFunctions {
	public static class MyWaterMarkAssigner implements AssignerWithPeriodicWatermarks<Tuple4<Integer,Integer,Integer,Long>> {
		private static final long serialVersionUID = 1L;
		private long maxTimestamp = 0;
		private long priorTimestamp = 0;

		@Override
		public Watermark getCurrentWatermark() {
			System.err.println("WM="+this.maxTimestamp);
			return new Watermark(this.maxTimestamp);
		}

		@Override
		public long extractTimestamp(Tuple4<Integer,Integer,Integer,Long> element, long previousElementTimestamp) {
			long millis = element.f3;
			maxTimestamp = Math.max(maxTimestamp, millis);
			// Date m = new Date(millis);

			return Long.valueOf(millis);
		}
	}
	@SuppressWarnings("serial")
	public static class SampleApplyFunction implements
			WindowFunction<Tuple4<Integer,Integer, Integer,Long>, Tuple4<Long, Long, List<Integer>, Integer>, Tuple, TimeWindow> {
		@Override
		public void apply(Tuple key, TimeWindow window, Iterable<Tuple4<Integer,Integer, Integer,Long>> inputs,
				Collector<Tuple4<Long, Long, List<Integer>, Integer>> out) throws Exception {
			System.err.println("Apply Thread Id"+Thread.currentThread().getId());
			List<Integer> eventIds = new ArrayList<>(0);
			int sum = 0;
			Iterator<Tuple4<Integer,Integer, Integer,Long>> iter = inputs.iterator();
			while (iter.hasNext()) {
				Tuple4<Integer,Integer, Integer,Long> input = iter.next();
				eventIds.add(input.f1);
				sum += input.f2;
			}

			out.collect(new Tuple4<>(window.getStart(), window.getEnd(), eventIds, sum));

		}
	}
}
