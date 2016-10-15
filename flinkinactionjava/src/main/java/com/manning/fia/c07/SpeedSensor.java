package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */

public class SpeedSensor extends Sensor {


    public SpeedSensor(int id, double reading, long timeStamp) {
        super(id, reading, timeStamp);
    }

    @Override
    String getType() {
        return "SPEED";
    }
}
