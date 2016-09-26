package com.manning.fia.newc06;

import com.manning.fia.utils.PriceParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.ArrayList;
import java.util.List;

/**

 */


public class ConnectedStreamsExample {

    private void executeJob(ParameterTool parameterTool) throws Exception {

        DataStream<Tuple2<String, Long>> rulesStream;
        DataStream<Tuple2<String, Double>> dataStream;
        ConnectedStreams<Tuple2<String, Double>, Tuple2<String, Long>> connectedStreams;
        DataStream<Tuple2<String,Double>> result;

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment
                .getExecutionEnvironment();

        final int parallelism = parameterTool.getInt("parallelism", 1);

        execEnv.setParallelism(parallelism);

        dataStream = execEnv.fromCollection
                (PriceParser.parseDataForTransactions("/purchases/pipe/price"));


        rulesStream = execEnv.fromCollection
                (PriceParser.parseDataForDiscounts("/purchases/pipe/discount"));


        connectedStreams = dataStream.connect(rulesStream);

        result=connectedStreams.keyBy(0, 0).map(new PriceDiscountCoMapper());

        // note we can do a flat map function , reduce to just display the price
        // as this is for illustration purpose of connected streams it will display both the prices.

        result.print();

        execEnv.execute("Connected Streams Example");
    }

    private static class PriceDiscountCoMapper implements CoMapFunction<Tuple2<String, Double>, Tuple2<String, Long>,
            Tuple2<String,Double>> {


        private double itemTransactionPrice = 0;

        @Override
        public Tuple2<String,Double> map1(Tuple2<String, Double> price) throws Exception {
            itemTransactionPrice = (price.f1);
            return new Tuple2<>(price.f0,itemTransactionPrice);
        }

        @Override
        public Tuple2<String,Double> map2(Tuple2<String, Long> discount) throws Exception {
            double itemDiscount = discount.f1;
            if (itemTransactionPrice > 0) {
                return new Tuple2<>(discount.f0,
                        itemTransactionPrice-((itemTransactionPrice * itemDiscount) / 100));
            }
            // this means the discount rule has fired first
            return new Tuple2<>(discount.f0,itemTransactionPrice);
        }
    }



    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        ConnectedStreamsExample window = new ConnectedStreamsExample();
        window.executeJob(parameterTool);
    }
}




