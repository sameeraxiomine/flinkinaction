package com.manning.fia.c07;

import java.io.Serializable;

/**
 * Created by hari on 10/15/16.
 */
public abstract class SensorEvent implements Serializable{

    public final int id;
    public final double reading;
    public final long timestamp;
    private int deviceId;

    public SensorEvent(int id, double reading, long timestamp) {
        this.id = id;
        this.reading = reading;
        this.timestamp = timestamp;
    }

     public int getDeviceId() {
		return deviceId;
  	 }

	public void setDeviceId(int deviceId) {
		this.deviceId = deviceId;
	}

	@Override
	public String toString() {
		return "SensorEvent [id=" + id + ", reading=" + reading + ", timestamp=" + timestamp + ", deviceId=" + deviceId
		      + "]";
	}

}
