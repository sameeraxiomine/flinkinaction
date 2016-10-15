package com.manning.fia.c07;

import java.io.Serializable;

/**
 * Created by hari on 10/15/16.
 */
public  class EngineControlModule implements Serializable{

    private int deviceId;
    
    public EngineControlModule(int deviceId) {
        this.deviceId = deviceId;        
    }

    public int getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(int deviceId) {
        this.deviceId = deviceId;
    }

    public SensorEvent receiveSensorEvents(SensorEvent sensor){
   	 sensor.setDeviceId(this.deviceId);
   	 return sensor;
    }
}
