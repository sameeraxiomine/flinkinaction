package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public abstract class Sensor {

    private int id;
    private double reading;
    private long timeStamp;
    private String type;

    public Sensor(int id, double reading, long timeStamp) {
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

    public double getReading() {
        return reading;
    }

    public void setReading(double reading) {
        this.reading = reading;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    abstract String getType();


    @Override
    public String toString() {
        return "Sensor{" +
                "id=" + id +
                ", reading=" + reading +
                ", timeStamp=" + timeStamp +
                ", type='" + type + '\'' +
                '}';
    }
}
