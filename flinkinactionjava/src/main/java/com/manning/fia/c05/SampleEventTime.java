package com.manning.fia.c05;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.joda.time.format.DateTimeFormat;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hari on 6/26/16.
 */
public class SampleEventTime {

    public void executeJob() throws Exception {
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();
        execEnv.setParallelism(5);
        execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        System.out.println("getAutoWatermarkInterval" + execEnv.getConfig().getAutoWatermarkInterval());
        DataStream<Tuple3<String,String,Long>> s1  = execEnv.addSource(new SampleDataSource(0)).assignTimestampsAndWatermarks(new TsWmGen());
        DataStream<Tuple3<String,String,Long>> s2  = execEnv.addSource(new SampleDataSource(20000)).assignTimestampsAndWatermarks(new TsWmGen());
           
        DataStream<Tuple3<String,String,Long>> s3 =  s1.union(s2);
        
        
        KeyedStream<Tuple3<String,String,Long>, Tuple> keyedDS = s3.keyBy(0);

        WindowedStream<Tuple3<String,String,Long>, Tuple, TimeWindow> windowedStream = keyedDS
                .timeWindow(Time.seconds(5));
        
        DataStream<Tuple3<Long, Long, List<String>>> result = windowedStream
                .apply(new MyApplyFunction());

        result.print();
        execEnv.execute("Tumbling Event Time Window Apply");

    }

    private static class TsWmGen
            implements
            AssignerWithPeriodicWatermarks<Tuple3<String, String,Long>> {
        private static final long serialVersionUID = 1L;
        private long wmTime = 0;

        @Override
        public Watermark getCurrentWatermark() {        		
            return new Watermark(wmTime);
        }

        @Override
        public long extractTimestamp(
                Tuple3<String, String, Long> element,
                long previousElementTimestamp) {
            long millis = element.f2;
            wmTime = Math.max(wmTime, millis);
            return Long.valueOf(millis);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Program Started at : "
                + DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
                        .print(System.currentTimeMillis()));
        SampleEventTime window = new SampleEventTime();
        window.executeJob();
    }
    
    public static class SampleDataSource implements SourceFunction<Tuple3<String,String,Long>>{
    	private List<Tuple3<String,String,Long>> data = new ArrayList<>();
    	private long threadSleepInterval = 0;
    	public SampleDataSource(long delay){
    		data.add(Tuple3.of("1","1", getDt("20160101000002")));
    		data.add(Tuple3.of("1","2", getDt("20160101000003")));
    		data.add(Tuple3.of("1","3", getDt("20160101000006")));
    		data.add(Tuple3.of("1","4", getDt("20160101000011")));
    		threadSleepInterval = delay;
    	}
        @Override
        public void run(SourceContext<Tuple3<String,String,Long>> sourceContext) throws Exception {
        	for(Tuple3 t:data){
        		sourceContext.collect(t);
        		Thread.currentThread().sleep(threadSleepInterval);
        	}        	
        }
        @Override
        public void cancel() { }
        private long getDt(String dt){
        	return DateTimeFormat.forPattern("yyyyMMddHHmmss")
            .parseDateTime(dt)
            .getMillis();
        }
    }
}
