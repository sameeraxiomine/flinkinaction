package com.manning.fia.c02;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

public class HashTagStreamSimulator implements SourceFunction<String> {
    private String[] sourcedata = null;
    private int pauseEveryIthIndex = Integer.MAX_VALUE;
    private int sleepIntervalInMillis = 1000;

    public HashTagStreamSimulator(String[] sourcedata, int pauseEveryIthIndex,
            int sleepIntervalInMillis) {
        this.sourcedata = sourcedata;
        this.pauseEveryIthIndex = pauseEveryIthIndex;
        this.sleepIntervalInMillis = sleepIntervalInMillis;
    }

    @Override
    public void cancel() {
        // TODO Auto-generated method stub

    }

    @Override
    public void run(SourceFunction.SourceContext<String> context)
            throws Exception {
        for (int i = 0; i < this.sourcedata.length; i++) {
            context.collect(sourcedata[i]);
            if ((i + 1) % this.pauseEveryIthIndex == 0) {
                //Thread.sleep(this.sleepIntervalInMillis);
                Thread.yield();
            }
        }
    }

}
