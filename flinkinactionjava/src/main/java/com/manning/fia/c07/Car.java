package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public abstract class Car {

    private int carId;
    private Long timeStamp;

    public Car(int carId, Long timeStamp) {
        this.carId = carId;
        this.timeStamp = timeStamp;
    }

    public int getCarId() {
        return carId;
    }

    public void setCarId(int carId) {
        this.carId = carId;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
