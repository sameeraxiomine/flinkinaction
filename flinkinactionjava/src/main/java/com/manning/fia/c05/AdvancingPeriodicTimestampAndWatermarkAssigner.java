package com.manning.fia.c05;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.format.DateTimeFormat;

public class AdvancingPeriodicTimestampAndWatermarkAssigner implements AssignerWithPeriodicWatermarks<Tuple5<Long, String, String, String, String>>{
   private static final long serialVersionUID = 1L;
   private long maxTimestamp = 0;           //#A
   private long priorMaxTimestamp = 0;      //#B
   private long lastTimeOfWaterMarking = 0l; //#C

   @Override
   public Watermark getCurrentWatermark() {
       if (maxTimestamp == priorMaxTimestamp) {
           long advance = (System.currentTimeMillis() - lastTimeOfWaterMarking); //#D
           maxTimestamp += advance;                                                      //#D
       }
       priorMaxTimestamp = maxTimestamp;                                                         //#E
       lastTimeOfWaterMarking = System.currentTimeMillis();
       return new Watermark(maxTimestamp-1);
   }

   @Override
   public long extractTimestamp(
           Tuple5<Long, String, String, String, String> element,
           long previousElementTimestamp) {
       long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
               .parseDateTime(element.f3).getMillis();                         //#B
       maxTimestamp = Math.max(maxTimestamp, millis);                                          //#C
       return Long.valueOf(millis);
   }
}

