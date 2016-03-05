package com.manning.chapter2

import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamingWordCount {
  def main(args: Array[String]) {

    val streamingEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = streamingEnvironment.readTextFile("src/main/resources/wordcount/input.txt")

    // Create a DataStream of <Word, Count>

    val counts = lines.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .sum(1)

    counts.writeAsText("/tmp/streamWordCount", FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    // Process the DataStream
    streamingEnvironment.execute("Streaming Word Count")
  }
}
