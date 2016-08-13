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

    public void executeJob(int parallelism, int noOfEvents,long standardDelayInSeconds, long anamolousDelayInSeconds, int watermarkEveryNInterval) throws Exception {
    	Map<Integer,List<Event>> data = new HashMap<>();
    	Map<Integer,Long> delay = new HashMap<>();
    	for(int i=0;i<(parallelism-1);i++){
        	delay.put(i, standardDelayInSeconds);
    	}
    	delay.put((parallelism-1), anamolousDelayInSeconds);
    	for(int i=0;i<parallelism;i++){
    		data.put(i,new ArrayList<Event>());
    	}
    	for(int i=0;i<noOfEvents;i++){
    		data.get(i%parallelism).add(new Event(i*1000+1,Tuple2.of(i%parallelism,i%parallelism+1)));
    	}
    	
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .createLocalEnvironment(parallelism);

        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> eventStream = execEnv.addSource(new SampleRichParallelSource(data,delay,watermarkEveryNInterval));

        DataStream<Tuple2<Integer,Integer>> selectDS = eventStream.map(new SimpleMapper());
        
        int windowInterval = 20;
        System.out.println("Window Interval " + windowInterval);
        selectDS.keyBy(0).timeWindow(Time.seconds(windowInterval)).sum(1).printToErr();

        execEnv.execute("Sample Event Time Pipeline");
    }


    public static void main(String[] args) throws Exception {        
        SampleParallelSourceStreamingPipeline window = new SampleParallelSourceStreamingPipeline();
        window.executeJob(5,100,1l,1l,2);

    }
    
    public static class SimpleMapper implements MapFunction<Event,Tuple2<Integer,Integer>> {

		@Override
		public Tuple2<Integer,Integer> map(Event value) throws Exception {
			// TODO Auto-generated method stub
			return value.getData();
		}
        
    }
}
