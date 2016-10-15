package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public class SpeedSensor extends Car{

    private int speedReading;

    public SpeedSensor(int carId,  int speedReading,Long timeStamp) {
        super(carId, timeStamp);
        this.speedReading = speedReading;
    }

    public int getSpeedReading() {
        return speedReading;
    }

    public void setSpeedReading(int speedReading) {
        this.speedReading = speedReading;
    }

    @Override
    public String toString() {
        return "SpeedSensor{" +
                "speedReading=" + speedReading +
                '}';
    }
}
