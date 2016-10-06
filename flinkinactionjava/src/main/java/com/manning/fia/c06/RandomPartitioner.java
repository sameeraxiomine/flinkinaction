package com.manning.fia.c06;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RandomPartitioner {

	public static void main(String[] args) throws Exception{
		  StreamExecutionEnvironment execEnv =
	            StreamExecutionEnvironment.createLocalEnvironment(20);
	      execEnv.setParallelism(2);
	      DataStream<Integer> source = execEnv.addSource(new RichParallelIntegerEventSource(4));
			source.shuffle().printToErr().setParallelism(2);
			execEnv.execute();
	}

}
