package com.manning.fia.c05;

import org.apache.flink.api.common.functions.MapFunction;

@SuppressWarnings("serial")
public class IdentityMapper<I> implements MapFunction<I,I> {

	@Override
	public I map(I value) throws Exception {
		// TODO Auto-generated method stub
		return value;
	}

}
