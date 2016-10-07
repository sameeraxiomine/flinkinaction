package com.manning.fia.c06;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomPartitionerWithPojos {

	public static List<MyPojo> getData(int size) {
		List<MyPojo> data = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			MyPojo pojo = new MyPojo(i);
			pojo.setI(i);
			data.add(pojo);
		}
		return data;
	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.createLocalEnvironment(20);
		int defaultParallelism = 8;
		execEnv.setParallelism(defaultParallelism);
		DataStream<MyPojo> source = execEnv.fromCollection(getData(8));
		Partitioner<Integer> partitioner = new Partitioner<Integer>() {
			@Override
			public int partition(Integer key, int numPartitions) {
				return ((key) / (8 / numPartitions));
			}
		};
		KeySelector<MyPojo, Integer> selector = new KeySelector<MyPojo, Integer>() {
			@Override
			public Integer getKey(MyPojo value) throws Exception {
				return value.getI();
			}

		};
		source.partitionCustom(partitioner, selector).printToErr().setParallelism(4);
		execEnv.execute();
	}

}
