package com.manning.fia.utils;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by hari on 9/25/16.
 */
public class SensorParser {

    public static List<Tuple3<String, Double, String>> parseData(String file) throws Exception {
        final Scanner scanner = new Scanner(SensorParser.class.getResourceAsStream(file));
        List<Tuple3<String, Double, String>> sensors = new ArrayList<>();
        while (scanner.hasNext()) {
            String value = scanner.nextLine();
            final String[] tokens = StringUtils.splitPreserveAllTokens(value, "|");
            sensors.add(new Tuple3<>(tokens[0], Double.valueOf(tokens[1]), tokens[2]));
        }
        return sensors;
    }
}
