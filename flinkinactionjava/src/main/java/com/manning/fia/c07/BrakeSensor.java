package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public class BrakeSensor extends EngineCarModule {

    private int id;
    private int reading;
    private long timeStamp;

    public BrakeSensor(int id, int reading, long timeStamp) {
        this.id = id;
        this.reading = reading;
        this.timeStamp = timeStamp;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public int getReading() {
        return reading;
    }

    public void setReading(int reading) {
        this.reading = reading;
    }

    @Override
    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
