package com.manning.fia.c06;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomPartitioner {

	public static void main(String[] args) throws Exception{
		  StreamExecutionEnvironment execEnv =
	            StreamExecutionEnvironment.createLocalEnvironment(20);
		  int defaultParallelism = 8;
	      execEnv.setParallelism(defaultParallelism);
	      DataStream<Tuple1<Integer>> source = execEnv.addSource(new RichParallelTuple1EventSource(1));
	      Partitioner<Integer> partitioner = new Partitioner<Integer>(){	      	
				@Override
				public int partition(Integer key, int numPartitions) {
					return ((key)/(defaultParallelism/numPartitions));
				}	      	
	      };
			source.partitionCustom(partitioner,0).printToErr().setParallelism(4);			
			execEnv.execute();
	}

}
