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

public class SampleRichParallelSourceNoTSOrWM extends RichParallelSourceFunction<Event> {

	private Map<Integer, List<Event>> data;
	private Map<Integer, Long> delaysBetweenEvents;
	private int index=-1;
	
	private long myDelay;
	public SampleRichParallelSourceNoTSOrWM(Map<Integer, List<Event>> data, Map<Integer, Long> delaysBetweenEvents) {
		this.data = data;
		this.delaysBetweenEvents = delaysBetweenEvents;
		
	}

	@Override
	public void open(Configuration configuration) {
		int numberTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		this.index = getRuntimeContext().getIndexOfThisSubtask();
		this.myDelay = this.delaysBetweenEvents.get(index)*1000;
		
	}

	@Override
	public void cancel() {

	}

	@Override
	public void run(SourceContext<Event> ctx) throws Exception {	
		Event e = null;
		List<Event> myData =data.get(this.index); 
		for(int i=0;i<myData.size();i++){
			e = myData.get(i);
			ctx.collect(e);
			//System.err.println(this.index + " waiting " + this.myDelay);
			Thread.currentThread().sleep(this.myDelay);
		}
		//Emit one more watermark;
		if(e!=null){
			//ctx.emitWatermark(new Watermark(e.getTimestamp()));
		}
	}

}
