package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public  class EngineComponentModule {

    private int deviceId;
    private Sensor sensor;

    public EngineComponentModule(int deviceId, Sensor sensor) {
        this.deviceId = deviceId;
        this.sensor = sensor;
    }

    public int getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(int deviceId) {
        this.deviceId = deviceId;
    }

    public Sensor getSensor() {
        return sensor;
    }

    public void setSensor(Sensor sensor) {
        this.sensor = sensor;
    }

    @Override
    public String toString() {
        return "EngineComponentModule{" +
                "deviceId=" + deviceId +
                ", sensor=" + sensor +
                '}';
    }
}
