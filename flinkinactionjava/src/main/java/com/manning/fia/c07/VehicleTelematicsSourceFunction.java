package com.manning.fia.c07;

import java.util.List;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created by hari on 10/15/16.
 */
public class VehicleTelematicsSourceFunction implements SourceFunction<SensorEvent> {

    private volatile boolean running=true;
    private int noOfEvents = 0;
    private List<SensorEvent> speedEvents = null;
    private List<SensorEvent> brakeEvents = null;

    public VehicleTelematicsSourceFunction(){
   	 CarData data = new CarData();	 
   	 this.noOfEvents = data.getNoOfEvents();
   	 this.speedEvents = data.getSpeedEvents();
   	 this.brakeEvents = data.getBrakeEvents();
    }
    
    @Override
    public void run(SourceContext<SensorEvent> ctx) throws Exception {
   	 long wmTs = 0;
   	 for(int i=0;i<noOfEvents;i++){
   		 SensorEvent speedEvent = this.speedEvents.get(i);
   		 SensorEvent brakeEvent = this.brakeEvents.get(i);
   		 ctx.collectWithTimestamp(speedEvent, speedEvent.timestamp);
   		 ctx.collectWithTimestamp(brakeEvent, brakeEvent.timestamp);
   		 wmTs = brakeEvent.timestamp-1;
   		 ctx.emitWatermark(new Watermark(wmTs));   		 
   		 Thread.currentThread().sleep(100);
   	 }
   	 ctx.emitWatermark(new Watermark(wmTs+1000));//Push the WM forward to flush all events to CEP
    }

    @Override
    public void cancel() {
        running=false;
    }
}
