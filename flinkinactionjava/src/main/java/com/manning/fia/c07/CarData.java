package com.manning.fia.c07;

import java.util.ArrayList;
import java.util.List;

public class CarData {
	private static int[] CAR_IDS = { 1, 2, 3, 4, 5 };
	
	private EngineControlModule[] ecms = new EngineControlModule[CAR_IDS.length];
	// SpeedLimit & TimeStamp
	private static SpeedSensorEvent[] SPEED_SENSOR = { new SpeedSensorEvent(100, 70, 5L),
	      new SpeedSensorEvent(100, 72, 10L), new SpeedSensorEvent(100, 68, 15L), new SpeedSensorEvent(100, 55, 20L),
	      new SpeedSensorEvent(100, 50, 25L), new SpeedSensorEvent(100, 54, 30L), new SpeedSensorEvent(100, 66, 35L),
	      new SpeedSensorEvent(100, 53, 40L), new SpeedSensorEvent(100, 65, 45L), new SpeedSensorEvent(100, 70, 50L) };

	// break range 0-10 (severity low-hig)and Timestamp

	private static BrakeSensorEvent[] BRAKE_SENSOR = { new BrakeSensorEvent(1000, 0, 5L),
	      new BrakeSensorEvent(1000, 0, 10L), new BrakeSensorEvent(1000, 8, 15L), new BrakeSensorEvent(1000, 4, 20L),
	      new BrakeSensorEvent(1000, 0, 25L), new BrakeSensorEvent(1000, 0, 30L), new BrakeSensorEvent(1000, 6, 35L),
	      new BrakeSensorEvent(1000, 0, 40L), new BrakeSensorEvent(1000, 0, 45L), new BrakeSensorEvent(1000, 0, 50L) };

	private List<SensorEvent> speedEvents = new ArrayList<SensorEvent>();
	private List<SensorEvent> brakeEvents = new ArrayList<SensorEvent>();

	public CarData() {
		for (int i = 0; i < CAR_IDS.length; i++) {
			ecms[i] = new EngineControlModule(CAR_IDS[i]);
		}
		for (int i = 0; i < SPEED_SENSOR.length; i++) {
			for (int j = 0; j < ecms.length; j++) {
				speedEvents.add(ecms[j].receiveSensorEvents(SPEED_SENSOR[i].copy()));
				brakeEvents.add(ecms[j].receiveSensorEvents(BRAKE_SENSOR[i].copy()));
			}
		}
	}

	public int getNoOfEvents() {
		return this.speedEvents.size();
	}



	public List<SensorEvent> getSpeedEvents() {
		return speedEvents;
	}


	public List<SensorEvent> getBrakeEvents() {
		return brakeEvents;
	}



}
