package com.manning.fia.transformations.media;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

@SuppressWarnings("serial")
public class ComputeTimeSpentPerSectionAndSubSection implements
        ReduceFunction<Tuple3<String, String, Long>> {
    @Override
    public Tuple3<String, String, Long> reduce(
            Tuple3<String, String, Long> value1,
            Tuple3<String, String, Long> value2) throws Exception {
        return new Tuple3<>(value1.f0, value1.f1, value1.f2 + value2.f2);
    }
}