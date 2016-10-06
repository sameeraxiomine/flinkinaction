package com.manning.fia.c06;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalancePartitioner {

	public static void main(String[] args) throws Exception{
      StreamExecutionEnvironment execEnv =
            StreamExecutionEnvironment.createLocalEnvironment(20);
      execEnv.setParallelism(1);      
      DataStream<Integer> source = execEnv.addSource(new RichParallelIntegerEventSource(4));
		source.rebalance().printToErr().setParallelism(2);
		execEnv.execute();
	}

}
