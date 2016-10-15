package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public class BrakeSensor extends Sensor {

    public BrakeSensor(int id, double reading, long timeStamp) {
        super(id, reading, timeStamp);
    }

    @Override
    String getType() {
        return "BRAKE";
    }
}
