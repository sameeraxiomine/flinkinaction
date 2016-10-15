package com.manning.fia.c07;

/**
 * Created by hari on 10/15/16.
 */
public class BrakeSensorEvent extends SensorEvent {

    public BrakeSensorEvent(int id, double reading, long timeStamp) {
        super(id, reading, timeStamp);
    }
    
    public BrakeSensorEvent copy(){
   	 return new BrakeSensorEvent(this.id,this.reading,this.timestamp);
    }
}
