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
import com.manning.transformation.StoreIdItemIdKeySelector3;
import com.manning.transformation.StoreIdItemIdKeySelector;
import com.manning.transformation.StoreIdKeySelector;
import com.manning.transformation.TransactionItemParser;

public class BasicTransformations {
    public static boolean RUN_LOCALLY = true;
    public static String HOST = "localhost";
    public static int PORT = 6123;
    public static String JAR_PATH = "target/flinkinactionjava-0.0.1-SNAPSHOT.jar";
    public static int DEFAULT_LOCAL_PARALLELISM = 1;
    public static int DEFAULT_REMOTE_PARALLELISM = 5;

    public static ExecutionEnvironment getEnvironment(boolean isLocal) {
        ExecutionEnvironment execEnv = null;
        if (isLocal) {
            execEnv = ExecutionEnvironment
                    .createLocalEnvironment(DEFAULT_LOCAL_PARALLELISM);
        } else {
            execEnv = ExecutionEnvironment.createRemoteEnvironment(HOST, PORT,
                    JAR_PATH);
            execEnv.setParallelism(DEFAULT_REMOTE_PARALLELISM);
        }
        return execEnv;
    }

    /*
     * 1. Map
     */
    public static void usingMap() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple7<Integer, Long, Integer, String, Integer, Double, Long>> tuples = source
                .map(new TransactionItemParser());
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = tuples
                .map(new ComputeTransactionValue());
        transformedTuples.print();
    }

    /*
     * 1.1 Map using Domain Object
     */
    public static void usingDomainObjectsMap() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<TransactionItem> projectedTuples = source.map(
                new DomainObjectBasedMap());
        projectedTuples.print();
    }

    /*
     * 2. Project
     */
    public static void usingProject() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple2<Long, String>> projectedTuples = source.map(
                new TransactionItemParser()).project(2, 3);
        projectedTuples.print();
    }

    /*
     * 3. Project with type hint
     */
    public static void usingProjectWithTypeHint() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple2<Long, String>> projectedTuples = source
                .map(new TransactionItemParser())
                .<Tuple2<Long, String>> project(2, 3).distinct();
        projectedTuples.print();
    }

    /*
     * 3.1 After Exercise 3.1 Filter
     */
    public static void usingFilter() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = source
                .map(new MapTokenizeAndComputeTransactionValue()).filter(
                        new FilterOnTransactionValue());
        transformedTuples.print();
    }

    /*
     * 4. Map Partition. On show after Exercise 3.1
     */
    public static void usingMapPartition() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = source
                .mapPartition(new MapPartitionTokenizeAndComputeTransactionValue());
        transformedTuples.print();
    }

    /*
     * 5. Flat Map - Do not discuss. Just point to the code. Mention Chapter 2
     */
    public static void usingFlatMap() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transformedTuples = source
                .flatMap(new FlatMapTokenizeAndComputeTransactionValue());
        transformedTuples.print();
    }

    /*
     * 5. Reduce - Only discuss Grouped Reduce in the book. Discuss
     * Shuffle/Partition at this point
     */
    public static void usingReduce() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new MapTokenizeAndComputeTransactionValue())
                .<Tuple3<Integer, Integer, Double>> project(0, 2, 4)
                .groupBy(0, 1)
                .reduce(new ComputeSumOfTransactionValueByStoreIdAndItemId());
        output.print();
    }

    /*
     * 6. Reduce using key selector. Only include the key selector code and
     * alter the group by clause
     * 
     * public static void usingKeySelectorBasedReduce() throws Exception {
     * ExecutionEnvironment execEnv = getEnvironment(true); DataSet<String>
     * source = execEnv.fromCollection(Arrays
     * .asList(SampleData.TRANSACTION_ITEMS)); DataSet<Tuple3<Integer, Integer,
     * Double>> output = source .map(new
     * MapTokenizeAndComputeTransactionValue()) .<Tuple3<Integer, Integer,
     * Double>> project(0, 2, 4) .groupBy(new StoreIdItemIdKeySelector3())
     * .reduce(new ComputeSumOfTransactionValueByStoreIdAndItemId());
     * output.print(); }
     */

    /*
     * 6. Group Reduce. Limitations of Reduce are it takes the same input and
     * output class GroupReduce allows different input and output classes
     * allowing us to work with domain objects Justification for KeySelector
     */
    public static void usingGroupReduce() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new DomainObjectBasedMap())
                .groupBy(new StoreIdItemIdKeySelector())
                .reduceGroup(
                        new GroupReduceSumOfTransactionValueByStoreIdAndItemId());
        output.print();
    }

    /*
     * 6. Group Reduce using Sorting.
     */
    public static void usingGroupReduceSortedKeys() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
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

    /*
     * 6. Group Reduce using Sorting with domain objects
     */
    public static void usingGroupReduceSortedKeysUsingKeySelector()
            throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
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

    /*
     * 7. Group Combine - Role of Combine (Point to MapReduce article)
     */
    public static void usingGroupCombine() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> combined = source
                .map(new DomainObjectBasedMap())
                .groupBy(new StoreIdItemIdKeySelector())
                .combineGroup(
                        new GroupCombineSumOfTransactionValueByStoreIdAndItemId());

        DataSet<Tuple3<Integer, Integer, Double>> output = combined.groupBy(0,
                1).reduceGroup(
                new SortedGroupReduceSumOfTransactionValueByStoreIdAndItemId());
        output.print();
    }

    /* Simple Aggregations Example. Point to Chapter 2 */

    public static void usingAggreatations() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        DataSet<Tuple3<Integer, Integer, Double>> output = source
                .map(new MapTokenizeAndComputeTransactionValue())
                .<Tuple3<Integer, Integer, Double>> project(0, 2, 4)
                .groupBy(0, 1).aggregate(Aggregations.SUM, 2);
        output.print();
    }

    /* Multiple Aggregations Example. Point to Chapter 2 */
    public static void usingMultipleAggreatations() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<String> source = execEnv.fromCollection(Arrays
                .asList(SampleData.TRANSACTION_ITEMS));
        /*
         * Per store find -minimum qty purchased in a transaction 
         * -most expensive item purchased in a transaction 
         * -sum of all transaction values
         */
        DataSet<Tuple4<Integer, Integer, Double, Double>> output = source
                .map(new MapTokenizeAndComputeTransactionValue2())
                .<Tuple4<Integer, Integer, Double, Double>> project(0, 3, 4, 5)
                .groupBy(0).aggregate(Aggregations.MIN, 1)
                .and(Aggregations.MAX, 2).and(Aggregations.SUM, 3);
        output.print();
    }

    /* Basic Join */
    public static void joinTransactionWithStoreBasic() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
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

    /* Basic Join better alternative */
    public static void joinTransactionWithStore() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
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

    /* Basic Join 2 */
    public static void joinTransactionWithStoreAndCustomer() throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<Tuple3<Integer, Long, Integer>> transactions = execEnv
                .fromCollection(Arrays.asList(SampleData.TRANSACTIONS))
                .map(new MapTokenizeTransaction()).project(0, 1, 2);
        DataSet<Tuple2<Integer, String>> stores = execEnv
                .fromCollection(Arrays.asList(SampleData.STORES))
                .map(new MapTokenizeStore()).project(0, 1);
        DataSet<Tuple3<Integer, String, String>> customers = execEnv
                .fromCollection(Arrays.asList(SampleData.CUSTOMERS))
                .map(new MapTokenizeCustomer()).project(0, 1, 2);
        DataSet<Tuple4<Integer, Long, Integer, String>> joinTransactionsStores = transactions
                .join(stores, JoinHint.BROADCAST_HASH_SECOND).where(0)
                .equalTo(0).projectFirst(0, 1, 2).projectSecond(1);
        DataSet<Tuple6<Integer, Long, Integer, String, String, String>> joinTransactionsStoresCustomers = joinTransactionsStores
                .join(customers, JoinHint.BROADCAST_HASH_SECOND).where(2)
                .equalTo(0).projectFirst(0, 1, 2, 3).projectSecond(1, 2);
        joinTransactionsStoresCustomers.print();
    }

    /* Basic Join Full */
    public static void joinTransactionWithTransactionItems(
            JoinHint bigJoinStrategy) throws Exception {
        ExecutionEnvironment execEnv = getEnvironment(true);
        DataSet<Tuple3<Integer, Long, Integer>> transactions = execEnv
                .fromCollection(Arrays.asList(SampleData.TRANSACTIONS))
                .map(new MapTokenizeTransaction()).project(0, 1, 2);
        DataSet<Tuple5<Integer, Long, Integer, String, Double>> transactionItems = execEnv
                .fromCollection(Arrays.asList(SampleData.TRANSACTION_ITEMS))
                .map(new MapTokenizeAndComputeTransactionValue());
        DataSet<Tuple2<Integer, String>> stores = execEnv
                .fromCollection(Arrays.asList(SampleData.STORES))
                .map(new MapTokenizeStore()).project(0, 1);
        DataSet<Tuple3<Integer, String, String>> customers = execEnv
                .fromCollection(Arrays.asList(SampleData.CUSTOMERS))
                .map(new MapTokenizeCustomer()).project(0, 1, 2);
        DataSet<Tuple4<Integer, Long, Integer, String>> joinTransactionsStores = transactions
                .join(stores, JoinHint.BROADCAST_HASH_SECOND).where(0)
                .equalTo(0).projectFirst(0, 1, 2).projectSecond(1);
        DataSet<Tuple6<Integer, Long, Integer, String, String, String>> joinTransactionsStoresCustomers = joinTransactionsStores
                .join(customers, JoinHint.BROADCAST_HASH_SECOND).where(2)
                .equalTo(0).projectFirst(0, 1, 2, 3).projectSecond(1, 2);

        DataSet<Tuple9<Integer, Long, Integer, String, String, String, String, String, Double>> joinAll = joinTransactionsStoresCustomers
                .join(transactionItems, bigJoinStrategy).where(0, 1)
                .equalTo(0, 1).projectFirst(0, 1, 2, 3, 4, 5)
                .projectSecond(2, 3, 4);
        joinAll.partitionByRange(0);
        joinAll.print();
    }

    public static void main(String[] args) throws Exception {
        BasicTransformations.usingDomainObjectsMap();
        // BasicTransformations.usingReduce();
        // BasicTransformations.usingKeySelectorBasedReduce();
        // BasicTransformations.usingGroupReduce();
        // BasicTransformations.usingGroupReduceSortedKeysUsingKeySelector();
        // BasicTransformations.usingGroupCombine();
        // BasicTransformations.usingAggreatations();
        // BasicTransformations.usingMultipleAggreatations();
        // BasicTransformations.joinTransactionWithStore();
        // BasicTransformations.joinTransactionWithStoreAndCustomer();
        // BasicTransformations.joinTransactionWithTransactionItems(JoinHint.REPARTITION_HASH_FIRST);
        // BasicTransformations.joinTransactionWithTransactionItems(JoinHint.REPARTITION_SORT_MERGE);
    }
}
