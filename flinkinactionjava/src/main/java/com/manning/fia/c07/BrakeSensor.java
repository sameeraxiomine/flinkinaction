package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public class BrakeSensor extends Car {

    private int breakReading;

    public BrakeSensor(int carId, int breakReading, Long timeStamp) {
        super(carId, timeStamp);
        this.breakReading = breakReading;
    }

    public int getBreakReading() {
        return breakReading;
    }

    public void setBreakReading(int breakReading) {
        this.breakReading = breakReading;
    }

    @Override
    public String toString() {
        return "BrakeSensor{" +
                "breakReading=" + breakReading +
                '}';
    }
}
