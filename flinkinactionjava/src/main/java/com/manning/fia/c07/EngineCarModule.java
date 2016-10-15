package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public abstract class EngineCarModule {

    private int carId;

    abstract long getTimeStamp();
    abstract int getReading();


    public int getCarId() {
        return carId;
    }

    public void setCarId(int carId) {
        this.carId = carId;
    }
}
