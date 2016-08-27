package com.manning.fia.c05;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.format.DateTimeFormat;

public class AscendingTimestampAndWatermarkAssigner
       extends AscendingTimestampExtractor<Tuple5<Long, String, String, String, String>>{   	 
   @Override
   public long extractAscendingTimestamp(Tuple5<Long, String, String, String, String> element) {
       long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss")
               .parseDateTime(element.f3).getMillis();
       return millis;
   }
}
