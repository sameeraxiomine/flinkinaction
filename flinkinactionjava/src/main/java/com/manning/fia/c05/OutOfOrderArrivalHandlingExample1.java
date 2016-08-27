package com.manning.fia.c05;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.manning.fia.utils.SampleSource;
import com.manning.fia.c05.C05HelperFunctions.BasicApplyFunction;

public class OutOfOrderArrivalHandlingExample1 {
	public static void main(String[] args) throws Exception {
		List<Tuple4<Integer, Integer, Integer, Long>> data = new ArrayList<>();
		data.add(Tuple4.of(0, 1, 1, 1999l));
		data.add(Tuple4.of(0, 2, 1, 2999l));
		data.add(Tuple4.of(0, 3, 1, 3999l));
		data.add(Tuple4.of(0, 11, 1, 6998l));
		data.add(Tuple4.of(0, 4, 1, 4999l));
		data.add(Tuple4.of(0, 12, 1, 6999l));
		
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);

		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStream<Tuple4<Integer, Integer, Integer, Long>> eventStream = execEnv.addSource(new SampleSource(data, 1000))
				                                                                    .setParallelism(1)
		                                                                          .assignTimestampsAndWatermarks(
		                                                                          new C05HelperFunctions.MyBoundedOutOfOrderedness(Time.seconds(2)));
		DataStream<Tuple4<Integer, Integer, Integer, Long>> selectDS = eventStream.project(0, 1, 2, 3);
		selectDS.keyBy(0).timeWindow(Time.seconds(5)).apply(new BasicApplyFunction()).printToErr();
		execEnv.execute("Out of Order Events Handled");
	}

}
