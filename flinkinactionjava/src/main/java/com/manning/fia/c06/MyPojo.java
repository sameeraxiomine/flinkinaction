package com.manning.fia.c06;

public class MyPojo {
	private int i=0;

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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + i;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MyPojo other = (MyPojo) obj;
		if (i != other.i)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MyPojo:"+Integer.toString(i);
	}
	
	
}