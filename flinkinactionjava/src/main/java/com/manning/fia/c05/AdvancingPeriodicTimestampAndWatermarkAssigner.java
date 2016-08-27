package com.manning.fia.c05;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.format.DateTimeFormat;

public class AdvancingPeriodicTimestampAndWatermarkAssigner implements AssignerWithPeriodicWatermarks<Tuple5<Long, String, String, String, String>>{
   private static final long serialVersionUID = 1L;
   private long maxTimestamp = 0;
   private long priorTimestamp = 0;
   private long lastTimeOfWaterMarking = 0l;

   @Override
   public Watermark getCurrentWatermark() {
       if (maxTimestamp == priorTimestamp) {
           long advance = (System.currentTimeMillis() - lastTimeOfWaterMarking);
           maxTimestamp += advance;// Start advancing
       }
       priorTimestamp = maxTimestamp;
       lastTimeOfWaterMarking = System.currentTimeMillis();
       return new Watermark(maxTimestamp);
   }

   @Override
   public long extractTimestamp(
           Tuple5<Long, String, String, String, String> element,
           long previousElementTimestamp) {
       long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
               .parseDateTime(element.f3).getMillis();
       maxTimestamp = Math.max(maxTimestamp, millis);
       return Long.valueOf(millis);
   }
}

