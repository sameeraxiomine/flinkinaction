package com.manning.fia.c05;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.manning.fia.utils.SampleSource;
import com.manning.fia.c05.LateArrivalHelperFunctions.SampleApplyFunction;

public class LateArrivalHandlingExample2 {
	public static void main(String[] args) throws Exception{
		//https://docs.google.com/document/d/1Xp-YBf87vLTduYSivgqWVEMjYUmkA-hyb4muX3KRl08/edit#
		List<Tuple4<Integer,Integer,Integer,Long>> data = new ArrayList<>();
				
		//data.add(new Event(7999, Tuple2.of(0, 100)));
		//Drop when WM>=END_TIME(4999)+LATENESS(2000)
		//data.add(new Event(6999, Tuple2.of(0, 1)));
		//data.add(Tuple4.of(0,1, 1,6999l));//All lost
		data.add(Tuple4.of(0,12, 1,6999l));
		data.add(Tuple4.of(0,11, 1,6998l));
		data.add(Tuple4.of(0,2, 1,4999l));
		data.add(Tuple4.of(0,3, 1,3999l));		
		data.add(Tuple4.of(0,4, 1,2999l));		
		data.add(Tuple4.of(0,5, 1,1999l));


		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(1);
		
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple4<Integer,Integer,Integer,Long>> eventStream = execEnv.addSource(new SampleSource(data,1000)).setParallelism(1)
				.assignTimestampsAndWatermarks(new LateArrivalHelperFunctions.MyWaterMarkAssigner());
		DataStream<Tuple4<Integer, Integer,Integer,Long>> selectDS = eventStream.project(0,1,2,3);
		selectDS.keyBy(0).timeWindow(Time.seconds(5)).allowedLateness(Time.milliseconds(2000)).apply(new SampleApplyFunction()).printToErr();
		execEnv.execute("Late Events Lost");
	}
	
	
}
