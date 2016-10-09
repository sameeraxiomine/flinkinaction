package com.manning.fia.c06;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;

public class MyOutputSelector implements OutputSelector<Tuple3<String, Integer, String>>{
	@Override
	public Iterable<String> select(Tuple3<String, Integer, String> value) {
		 List<String> output = new ArrayList<String>();
		 String type = value.f2;
		 output.add(type);
        return output;
	}      		
}


