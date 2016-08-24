package com.manning.fia.utils;

import java.util.List;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import com.manning.fia.utils.Event;
/**
 * This class will poll a folder every 5 seconds
 */

public class SimpleRichParallelSource extends RichParallelSourceFunction<Event> {

	private List<Event> data;
	private long myDelay;
	public SimpleRichParallelSource(List<Event> data, long myDelay) {
		this.data = data;
		this.myDelay = myDelay;

	}

	@Override
	public void open(Configuration configuration) {
		
		
	}

	@Override
	public void cancel() {

	}

	@Override
	public void run(SourceContext<Event> ctx) throws Exception {	
		//System.err.println("Source Thread Id"+Thread.currentThread().getId());
		for(Event e:this.data){
			//System.err.println("Source Thread Id"+Thread.currentThread().getId());
			ctx.collect(e);
			Thread.currentThread().sleep(this.myDelay);
		}
	}

}
