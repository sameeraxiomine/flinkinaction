package com.manning.fia.utils;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by hari on 7/17/16.
 */
public class SampleSource implements SourceFunction<Tuple4<Integer,Integer,Integer,Long>>{
	private long delay=0;
	private List<Tuple4<Integer,Integer,Integer,Long>> data=new ArrayList<>();
    public SampleSource(List<Tuple4<Integer,Integer,Integer,Long>>data) {
    	
        this.data = data;
    }
    public SampleSource(List<Tuple4<Integer,Integer,Integer,Long>>data,long delay) {
    	this.delay = delay;
        this.data = data;
    }

    @Override
    public void run(SourceContext<Tuple4<Integer,Integer,Integer,Long>> ctx) throws Exception {
    	Thread.currentThread().sleep(delay);
    	for(Tuple4<Integer,Integer,Integer,Long> e:this.data){
    		ctx.collect(e);
    		Thread.currentThread().sleep(delay);
    	}
    }

    @Override
    public void cancel() {

    }

}
