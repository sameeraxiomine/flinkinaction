package com.manning.fia.utils;

import org.apache.flink.api.java.utils.ParameterTool;

public class UsingTheParameterTool {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String topic = parameterTool.getRequired("topic");
        int i = parameterTool.getInt("i", 0);
        boolean b = parameterTool.getBoolean("b", false);
    }
}