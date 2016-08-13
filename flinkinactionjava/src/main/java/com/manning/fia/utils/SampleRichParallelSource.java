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

public class SampleRichParallelSource extends RichParallelSourceFunction<Event> {

	private Map<Integer, List<Event>> data;
	private Map<Integer, Long> delaysBetweenEvents;
	private int index=-1;
	private int watermarkEventNEvents=5;
	private long myDelay;
	public SampleRichParallelSource(Map<Integer, List<Event>> data, Map<Integer, Long> delaysBetweenEvents, int watermarkEventNEvents) {
		this.data = data;
		this.delaysBetweenEvents = delaysBetweenEvents;
		this.watermarkEventNEvents = watermarkEventNEvents;
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
			ctx.collectWithTimestamp(e,e.getTimestamp());
			//System.err.println(this.index + " waiting " + this.myDelay);
			Thread.currentThread().sleep(this.myDelay);
			if(i%this.watermarkEventNEvents==0){
				//System.err.println(this.index + " now emitting watermark " );
				ctx.emitWatermark(new Watermark(e.getTimestamp()));
			}
		}
		//Emit one more watermark;
		if(e!=null){
			ctx.emitWatermark(new Watermark(e.getTimestamp()));
		}
	}

}
