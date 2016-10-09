package com.manning.fia.c06;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class MyOutputSelector implements OutputSelector<Tuple4<String, Integer, String, String>>{
	@Override
	public Iterable<String> select(Tuple4<String, Integer, String, String> value) {
		 List<String> output = new ArrayList<String>();
		 String type = value.f2;
		 output.add(type);
        return output;
	}      		
}


