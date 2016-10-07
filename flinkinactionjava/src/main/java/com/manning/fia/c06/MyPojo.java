package com.manning.fia.c06;

public class MyPojo {
	private int i=0;
	public MyPojo() {
	}
	public MyPojo(int i) {
		this.i = i;
	}

	public void setI(int i) {
		this.i = i;
	}

	public int getI(){
		return i;
	}

	@Override
	public String toString() {
		return "MyPojo:"+Integer.toString(i);
	}
	
	
}