package com.manning;

import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.TimeZone;

public class Sample {
    public static void main(String[] args) throws Exception{
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmm");        
        System.out.println(inputFormat.parse("201603011212"));
    }
}
