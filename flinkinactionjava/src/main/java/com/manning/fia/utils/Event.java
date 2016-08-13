package com.manning.fia.utils;

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

public class Event implements Serializable{
	private long timestamp=0;
	private Tuple2 data = null;
	public Event(long timestamp, Tuple2 data) {
		super();
		this.timestamp = timestamp;
		this.data = data;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public Tuple2 getData() {
		return data;
	}

}
