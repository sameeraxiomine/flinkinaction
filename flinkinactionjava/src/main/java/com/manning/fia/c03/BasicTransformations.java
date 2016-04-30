package com.manning.fia.c03;

import java.util.Arrays;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;

import com.manning.model.petstore.TransactionItem;
import com.manning.parsers.TransactionItemParser;
import com.manning.transformation.ComputeSumOfTransactionValueByStoreIdAndItemId;
import com.manning.transformation.ComputeTransactionValue;
import com.manning.transformation.DomainObjectBasedMap;
import com.manning.transformation.FilterOnTransactionValue;
import com.manning.transformation.FlatMapTokenizeAndComputeTransactionValue;
import com.manning.transformation.GroupCombineSumOfTransactionValueByStoreIdAndItemId;
import com.manning.transformation.GroupReduceSumOfTransactionValueByStoreIdAndItemId;
import com.manning.transformation.ItemIdKeySelector;
import com.manning.transformation.MapPartitionTokenizeAndComputeTransactionValue;
import com.manning.transformation.MapTokenizeAndComputeTransactionValue;
import com.manning.transformation.MapTokenizeAndComputeTransactionValue2;
import com.manning.transformation.MapTokenizeCustomer;
import com.manning.transformation.MapTokenizeStore;
import com.manning.transformation.MapTokenizeTransaction;
import com.manning.transformation.SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId;
import com.manning.transformation.SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId2;
import com.manning.transformation.StoreIdItemIdKeySelector;
import com.manning.transformation.StoreIdItemIdKeySelector2;
import com.manning.transformation.StoreIdKeySelector;

public class BasicTransformations {
    public static void usingMap() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
                .map(new TransactionItemParser());
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = tuples
                .map(new ComputeTransactionValue());
        transformedTuples.print();
    }

    public static void usingProject() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple2<Long, String>> projectedTuples = source.map(
                new TransactionItemParser()).project(2, 3);
        projectedTuples.print();
    }

    public static void usingProjectWithTypeHint() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple2<Long, String>> projectedTuples = source
                .map(new TransactionItemParser())
                .<Tuple2<Long, String>> project(2, 3).distinct();
        projectedTuples.print();
    }

    /*
     * Needs and explanation of what is a partition. Develop a blog article
     */
    public static void usingMapPartition() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = source
                .mapPartition(new MapPartitionTokenizeAndComputeTransactionValue());
        transformedTuples.print();
    }

    /*
     * Do not discuss. Just point to the code
     */
    public static void usingFlatMap() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = source
                .flatMap(new FlatMapTokenizeAndComputeTransactionValue());
        transformedTuples.print();
    }

    public static void usingFilter() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = source
                .map(new MapTokenizeAndComputeTransactionValue()).filter(
                        new FilterOnTransactionValue());
        transformedTuples.print();
    }

    public static void usingDomainObjectsMap() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<TransactionItem> projectedTuples = source
                .map(new DomainObjectBasedMap());
        projectedTuples.print();
    }

    public static void usingReduce() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new MapTokenizeAndComputeTransactionValue())
                .<Tuple3<Integer, Integer, Double>> project(0, 2, 4)
                .groupBy(0, 1)
                .reduce(new ComputeSumOfTransactionValueByStoreIdAndItemId());
        output.print();
    }

    public static void usingKeySelectorBasedReduce() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new MapTokenizeAndComputeTransactionValue())
                .<Tuple3<Integer, Integer, Double>> project(0, 2, 4)
                .groupBy(new StoreIdItemIdKeySelector())
                .reduce(new ComputeSumOfTransactionValueByStoreIdAndItemId());
        output.print();
    }

    public static void usingGroupReduce() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new DomainObjectBasedMap())
                .groupBy(new StoreIdItemIdKeySelector2())
                .reduceGroup(
                        new GroupReduceSumOfTransactionValueByStoreIdAndItemId());
        output.print();
    }

    public static void usingGroupReduceSortedKeys() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new MapTokenizeAndComputeTransactionValue())
                .<Tuple3<Integer, Integer, Double>> project(0, 2, 4)
                .groupBy(0)
                .sortGroup(1, Order.ASCENDING)
                .reduceGroup(
                        new SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId());
        output.print();
    }

    public static void usingGroupCombine() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> combined = source
                .map(new DomainObjectBasedMap())
                .groupBy(new StoreIdItemIdKeySelector2())
                .combineGroup(
                        new GroupCombineSumOfTransactionValueByStoreIdAndItemId());

        DataSet<Tuple3<Integer, Integer, Double>> output = combined.groupBy(0,
                1).reduceGroup(
                new SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId());
        output.print();
    }

    public static void usingGroupReduceSortedKeysUsingKeySelector()
            throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new DomainObjectBasedMap())
                .groupBy(new StoreIdKeySelector())
                .sortGroup(new ItemIdKeySelector(), Order.ASCENDING)
                .reduceGroup(
                        new SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId2());
        output.print();
    }

    public static void usingAggreatations() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new MapTokenizeAndComputeTransactionValue())
                .<Tuple3<Integer, Integer, Double>> project(0, 2, 4)
                .groupBy(0, 1).aggregate(Aggregations.SUM, 2);
        output.print();
    }

    public static void usingMultipleAggreatations() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));

        /*
         * Per store find -minimum qty purchased in a transaction -most
         * expensive item purchased in a transaction -sum of all transaction
         * values
         */
        DataSet<Tuple4<Integer, Integer, Double, Double>> output = source
                .map(new MapTokenizeAndComputeTransactionValue2())
                .<Tuple4<Integer, Integer, Double, Double>> project(0, 3, 4, 5)
                .groupBy(0).aggregate(Aggregations.MIN, 1)
                .and(Aggregations.MAX, 2).and(Aggregations.SUM, 3);
        output.print();
    }

    public static void joinTransactionWithStoreBasic() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<Tuple3<Integer, Long, Integer>> transactions = execEnv
                .fromCollection(Arrays.asList(SampleData.TRANSACTIONS))
                .map(new MapTokenizeTransaction()).project(0, 1, 2);
        DataSet<Tuple2<Integer, String>> stores = execEnv
                .fromCollection(Arrays.asList(SampleData.STORES))
                .map(new MapTokenizeStore()).project(0, 1);
        DataSet<Tuple2<Tuple3<Integer, Long, Integer>, Tuple2<Integer, String>>> joinTransactionsStores = transactions
                .join(stores, JoinHint.BROADCAST_HASH_SECOND).where(0)
                .equalTo(0);

        joinTransactionsStores.print();
    }

    public static void joinTransactionWithStore() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<Tuple3<Integer, Long, Integer>> transactions = execEnv
                .fromCollection(Arrays.asList(SampleData.TRANSACTIONS))
                .map(new MapTokenizeTransaction()).project(0, 1, 2);
        DataSet<Tuple2<Integer, String>> stores = execEnv
                .fromCollection(Arrays.asList(SampleData.STORES))
                .map(new MapTokenizeStore()).project(0, 1);
        DataSet<Tuple4<Integer, Long, Integer, String>> joinTransactionsStores = transactions
                .join(stores, JoinHint.BROADCAST_HASH_SECOND).where(0)
                .equalTo(0).projectFirst(0, 1, 2).projectSecond(1);
        joinTransactionsStores.print();
    }

    public static void joinTransactionWithStoreAndCustomer() throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<Tuple3<Integer, Long, Integer>> transactions = execEnv
                .fromCollection(Arrays.asList(SampleData.TRANSACTIONS))
                .map(new MapTokenizeTransaction())
                .project(0, 1, 2);
        DataSet<Tuple2<Integer, String>> stores = execEnv.fromCollection(
                Arrays.asList(SampleData.STORES)).map(new MapTokenizeStore()).project(0, 1);
        DataSet<Tuple3<Integer,String, String>> customers = execEnv.fromCollection(
                Arrays.asList(SampleData.CUSTOMERS)).map(new MapTokenizeCustomer()).project(0, 1, 2);
        DataSet<Tuple4<Integer, Long, Integer,String>> joinTransactionsStores = transactions
                .join(stores, JoinHint.BROADCAST_HASH_SECOND).where(0)
                .equalTo(0)
                .projectFirst(0,1,2)
                .projectSecond(1);
        DataSet<Tuple6<Integer, Long, Integer,String,String,String>> joinTransactionsStoresCustomers = joinTransactionsStores
                .join(customers, JoinHint.BROADCAST_HASH_SECOND).where(2)
                .equalTo(0)
                .projectFirst(0,1,2,3)
                .projectSecond(1,2);
        joinTransactionsStoresCustomers.print();
    }

    public static void joinTransactionWithTransactionItems(JoinHint bigJoinStrategy) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment
                .getExecutionEnvironment();
        DataSet<Tuple3<Integer, Long, Integer>> transactions = execEnv
                .fromCollection(Arrays.asList(SampleData.TRANSACTIONS))
                .map(new MapTokenizeTransaction())
                .project(0, 1, 2);
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transactionItems = execEnv
                .fromCollection(Arrays.asList(SampleData.TRANSACTION_ITEMS))
                .map(new MapTokenizeAndComputeTransactionValue());
        DataSet<Tuple2<Integer, String>> stores = execEnv.fromCollection(
                Arrays.asList(SampleData.STORES)).map(new MapTokenizeStore()).project(0, 1);
        DataSet<Tuple3<Integer,String, String>> customers = execEnv.fromCollection(
                Arrays.asList(SampleData.CUSTOMERS)).map(new MapTokenizeCustomer()).project(0, 1, 2);
        DataSet<Tuple4<Integer, Long, Integer,String>> joinTransactionsStores = transactions
                .join(stores, JoinHint.BROADCAST_HASH_SECOND).where(0)
                .equalTo(0)
                .projectFirst(0,1,2)
                .projectSecond(1);
        DataSet<Tuple6<Integer, Long, Integer,String,String,String>> joinTransactionsStoresCustomers = joinTransactionsStores
                .join(customers, JoinHint.BROADCAST_HASH_SECOND).where(2)
                .equalTo(0)
                .projectFirst(0,1,2,3)
                .projectSecond(1,2);
        
        DataSet<Tuple9<Integer, Long, Integer,String,String,String,String,String,Double>> joinAll = joinTransactionsStoresCustomers
                .join(transactionItems, bigJoinStrategy).where(0,1)                
                .equalTo(0,1)                
                .projectFirst(0,1,2,3,4,5)
                .projectSecond(2,3,4);                
        joinAll.partitionByRange(0);
        joinAll.print();
    }

    public static void main(String[] args) throws Exception {
        // BasicTransformations.usingReduce();
        // BasicTransformations.usingKeySelectorBasedReduce();
        // BasicTransformations.usingGroupReduce();
        // BasicTransformations.usingGroupReduceSortedKeysUsingKeySelector();
        // BasicTransformations.usingGroupCombine();
        // BasicTransformations.usingAggreatations();
        // BasicTransformations.usingMultipleAggreatations();
        //BasicTransformations.joinTransactionWithStore();
        //BasicTransformations.joinTransactionWithStoreAndCustomer();
        BasicTransformations.joinTransactionWithTransactionItems(JoinHint.REPARTITION_HASH_FIRST);
        BasicTransformations.joinTransactionWithTransactionItems(JoinHint.REPARTITION_SORT_MERGE);
    }
}
