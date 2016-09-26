package com.manning.fia.utils;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by hari on 9/25/16.
 */
public class PriceParser {

    public static List<Tuple2<String, Long>> parseDataForDiscounts(String file) throws Exception {
        final Scanner scanner = new Scanner(PriceParser.class.getResourceAsStream(file));
        List<Tuple2<String, Long>> discounts = new ArrayList<>();
        while (scanner.hasNext()) {
            String value = scanner.nextLine();
            final String[] tokens = StringUtils.splitPreserveAllTokens(value, "|");
            discounts.add(new Tuple2<>(tokens[0], Long.valueOf(tokens[1])));
        }
        return discounts;
    }

    public static List<Tuple2<String, Double>> parseDataForTransactions(String file) throws Exception {
        final Scanner scanner = new Scanner(PriceParser.class.getResourceAsStream(file));
        List<Tuple2<String, Double>> transactions = new ArrayList<>();
        while (scanner.hasNext()) {
            String value = scanner.nextLine();
            final String[] tokens = StringUtils.splitPreserveAllTokens(value, "|");
            transactions.add(new Tuple2<>(tokens[0], Double.valueOf(tokens[1])));
        }
        return transactions;
    }


}
