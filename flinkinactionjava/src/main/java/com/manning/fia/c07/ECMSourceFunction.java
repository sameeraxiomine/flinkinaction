package com.manning.fia.c07;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created by hari on 10/15/16.
 */
public class ECMSourceFunction implements ParallelSourceFunction<EngineComponentModule> {

    volatile boolean running=true;

    private static int[] DEVICE_ID ={1};
    // SpeedLimit & TimeStamp
    private static SpeedSensor[] SPEED_SENSOR = {
            new SpeedSensor(100,70, 5L),
            new SpeedSensor(100,72, 10L),
            new SpeedSensor(100,68, 15L),
            new SpeedSensor(100,55, 20L),
            new SpeedSensor(100,50, 25L),
            new SpeedSensor(100,54, 30L),
            new SpeedSensor(100,66, 35L),
            new SpeedSensor(100,53, 40L),
            new SpeedSensor(100,65, 45L),
            new SpeedSensor(100,70, 50L)
    };

    //break range 0-10 (severity low-hig)and Timestamp

    private static BrakeSensor[] BRAKE_SENSOR = {
            new BrakeSensor(1000,0, 5L),
            new BrakeSensor(1000,0, 10L),
            new BrakeSensor(1000,8, 15L),
            new BrakeSensor(1000,4, 20L),
            new BrakeSensor(1000,0, 25L),
            new BrakeSensor(1000,0, 30L),
            new BrakeSensor(1000,6, 35L),
            new BrakeSensor(1000,0, 40L),
            new BrakeSensor(1000,0, 45L),
            new BrakeSensor(1000,0, 50L)
    };

    @Override
    public void run(SourceContext<EngineComponentModule> ctx) throws Exception {



        for (int idCounter=0;idCounter<DEVICE_ID.length;idCounter++) {
            int deviceId= DEVICE_ID[idCounter];
            for (int i=0;i<SPEED_SENSOR.length;i++) {
                //FOR SPEED
                Sensor speedSensor = SPEED_SENSOR[i];
                EngineComponentModule engineComponentModule = new EngineComponentModule(deviceId,
                        speedSensor);

                ctx.collectWithTimestamp(engineComponentModule, speedSensor.getTimeStamp());
                ctx.emitWatermark(new Watermark(speedSensor.getTimeStamp()));

                // FOR BRAKE
                Sensor brakeSensor = BRAKE_SENSOR[i];
                EngineComponentModule engineComponentModule1 = new EngineComponentModule(deviceId,
                        brakeSensor);
                ctx.collectWithTimestamp(engineComponentModule1, brakeSensor.getTimeStamp());

                ctx.emitWatermark(new Watermark(brakeSensor.getTimeStamp()));

            }
        }


    }

    @Override
    public void cancel() {
        running=false;
    }
}
