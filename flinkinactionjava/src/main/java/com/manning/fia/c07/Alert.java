package com.manning.fia.c07;

import java.io.Serializable;
import java.util.Map;

public class Alert implements Serializable{
	private SensorEvent first;
	private SensorEvent next;
	private SensorEvent end;
	public Alert(SensorEvent first,SensorEvent next,SensorEvent end){
		this.first = first;
		this.next = next;
		this.end = end;
	}
	@Override
	public String toString() {
		return "Alert [first=" + first + ", next=" + next + ", end=" + end + "]";
	}
	
	
}
