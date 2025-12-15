package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object TopKApp {
  def main(args: Array[String]) : Unit = {
    if (args.length < 2) {
      System.err.println("Usage: TopKApp <input-path> <output-path>")
      System.exit(1)
    }

    val inputPath = args(0)
    val outputPath = args(1)


    val conf = new SparkConf().setAppName("Top30IPs")
    val sc = new SparkContext(conf)


    // Read raw log lines
    val lines: RDD[String] = sc.textFile(inputPath)

    // Parse IP and status code
    val validIPs: RDD[String] = lines.flatMap { line =>
      val parts = line.split(" ")
      if (parts.length < 10) {
        None
      } else {
        val ip = parts(0)
        val statusCode = parts(parts.length - 2) // second-to-last field

        // Skip non-200, private IPs
        if (statusCode == "200" && !ip.startsWith("127.") && !ip.startsWith("10.")) {
          Some(ip)
        } else {
          None
        }
      }
    }

    // Count occurrences
    val ipCounts: RDD[(String, Int)] = validIPs
      .map(ip => (ip, 1))
      .reduceByKey(_ + _)

    // Sort by count (descending) and take top 30
    val top30: Array[(String, Int)] = ipCounts
      .takeOrdered(30)(Ordering.by[(String, Int), Int](-_._2))

    // Save result to output path as text file
    val outputRDD: RDD[String] = sc.parallelize(
      top30.zipWithIndex.map { case ((ip, count), idx) =>
      val rank = idx + 1
      s"$rank\t$ip\t$count"
    })

    outputRDD.saveAsTextFile(outputPath)

    println(s"Top 30 IPs saved to: $outputPath")
    sc.stop()
  }
}
