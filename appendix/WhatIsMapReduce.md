# Basics of MapReduce #
In this article provides a very high level primer on MapReduce. We will use the Word Count example to explain the concepts of MapReduce.

## MapReduce components ##
The key components of a MapRedue to solve the WordCount program are illustrated in Figure 1.

![](../images/WhatIsMapReduce.png)
Figure 1: WordCount using MapReduce

The key components of MapReduce are:

1. **A Distributed File System** - Large file or a set of files is placed on a distributed file system like HDFS. 
2. **Splitter** - The large files are split into smaller splits of pre-determined size (ex. 128 MB). A actual split may be slightly larger to ensure that each split has whole records. In our example, a record is one line in the input file.
3. **Mapper** - A Mapper component processes one split and each `map` call processes one record at a time. The output of a map invocation a key-value pair. In our case the output of a `map` call is `<word,1>`
4. **Shuffler** - The shuffle phase will collect all key-value pairs from all the Mappers and group the values by key. It will assign a key and its group of values to a Reducer component. The number of Reducer components are known before the job initiates. Reducer components can (and usually do) execute one different machines.  
5. **Reducer** - The grouping of key-values (note the plural) will arrive at a Reducer and a reduce call is invoked once per key-values pair. The result will be an output key-value pair. In our case it is the total count per word.  

## WordCount in Flink ##

A Flink program which implements WordCount is shown in Listing 1. 
    
```
ExecutionEnvironment execEnv = ExecutionEvironment.getExecutionEnvironment();

//Read from the input path - params.get("input")
DataSet<String> text = = execEnv.readTextFile(params.get("input"));
DataSet<Tuple2<String, Integer>> counts = 
// split up the lines in pairs (2-tuples) containing: (word,1)
text.flatMap(new Tokenizer())
// group by the tuple field "0" and sum up tuple field "1"
.groupBy(0)
.sum(1);
//Write to the output path - params.get("output")
counts.writeAsCsv(params.get("output"), "\n", " ");
```
Listing 1: WordCount in Flink

The components of the program and how they map to the MapReduce components are illustrated below:

1. **Read from source and generate splits** - Read the file(s) from the input path(folder) and create a DataSet<String> instance. The splitter component is contained in the function call which reads the input. 
2. **Map operation** - Use a FlatMap operator to convert each line to a pair - (word,1)
3. **Shuffle operation** - Group the output of FlatMap operator by word 
4. **Reduce operation** - Add up the 1's for each word and produce a final count for each word
5. **Write to Sink** - Write to the output path

