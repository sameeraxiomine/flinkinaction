package com.manning.fia.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.transformations.media.NewsFeedParser;
import com.manning.fia.utils.DateUtils;
import com.manning.fia.utils.Event;
import com.manning.fia.utils.NewsFeedSocket;
import com.manning.fia.utils.SampleRichParallelSource;
/**
 * Created by hari on 6/26/16.
 */
public class SampleParallelSourceStreamingPipeline {

    public void executeJob() throws Exception {
    	int parallelism = 5;
    	
    	Map<Integer,List<Event>> data = new HashMap<>();
    	Map<Integer,Long> delay = new HashMap<>();
    	delay.put(0, 1l);
    	delay.put(1, 1l);
    	delay.put(2, 1l);
    	delay.put(3, 1l);
    	delay.put(4, 1l);
    	
    	for(int i=0;i<parallelism;i++){
    		data.put(i,new ArrayList<Event>());
    	}
    	for(int i=0;i<100;i++){
    		data.get(i%parallelism).add(new Event(i*1000,Tuple2.of(i%parallelism,i%parallelism+1)));
    	}
    	
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(parallelism);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> eventStream = execEnv.addSource(new SampleRichParallelSource(data,delay,5));

        DataStream<Tuple2<Integer,Integer>> selectDS = eventStream.map(new SimpleMapper());
                
        selectDS.keyBy(0).timeWindow(Time.seconds(5)).sum(1).printToErr();

        execEnv.execute("Sample Event Time Pipeline");
    }


    public static void main(String[] args) throws Exception {        
        SampleParallelSourceStreamingPipeline window = new SampleParallelSourceStreamingPipeline();
        window.executeJob();

    }
    
    public static class SimpleMapper implements MapFunction<Event,Tuple2<Integer,Integer>> {

		@Override
		public Tuple2<Integer,Integer> map(Event value) throws Exception {
			// TODO Auto-generated method stub
			return value.getData();
		}
        
    }
}
