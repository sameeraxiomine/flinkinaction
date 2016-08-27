package com.manning.fia.c05;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.format.DateTimeFormat;

public class PeriodicTimestampAndWatermarkAssigner implements AssignerWithPeriodicWatermarks<Tuple5<Long, String, String, String, String>>{
	 private long maxTimestamp=0;

	 @Override
	 public long extractTimestamp(Tuple5<Long, String, String, String, String> element,
	                              long previousElementTimestamp) {
     long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
           .parseDateTime(element.f3).getMillis();
     this.maxTimestamp = Math.max(millis, maxTimestamp);
     return millis;		
  }

	@Override
	public Watermark getCurrentWatermark() {		
		return new Watermark(this.maxTimestamp-1);
	}
}
