package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import java.io._
import utils._

import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-0-rollup")
      //.master("local[*]")
      .getOrCreate()

    // val inputSmall = new File(getClass.getResource("/lineorder_small.tbl").getFile).getPath
    val dfSmall = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load("/user/cs422/lineorder_small.tbl")
      .repartition(16)
    // val inputMedium = new File(getClass.getResource("/lineorder_medium.tbl").getFile).getPath
    val dfMedium = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load("/user/cs422/lineorder_medium.tbl")
      .repartition(16)
    // val inputBig = new File(getClass.getResource("/lineorder_big.tbl").getFile).getPath
    val dfBig = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load("/user/cs422/lineorder_big.tbl")
      .repartition(16)

    val attribute = 8
    val operation = "SUM"
    val rollup = new RollupOperator

    for {
      (df, dataset) <- List((dfSmall, "small"), (dfMedium, "medium"), (dfBig, "big"))
      groupingList <- List(List(0), List(0,1), List(0,1,3), List(0,1,2,3), List(0,1,2,3,4), List(0,1,2,3,4,9))
    } {
      println("- Rollup on " + dataset + ", attribute " + attribute + ", grouping " + groupingList + ", operation " + operation)

      val rdd = df.rdd
      var resNaive : RDD[(List[Any], Double)] = null
      for (i <- (0 until 5)) {
        val timeNaive = Utils.getTimingInMs(() => {
          resNaive = rollup.rollup_naive(rdd, groupingList, attribute, operation)
          resNaive.count()
        })
        println("--- Time naive: " + timeNaive + "ms")
      }

      var resOptimized : RDD[(List[Any], Double)] = null
      for (i <- (0 until 5)) {
        val timeOptimized = Utils.getTimingInMs(() => {
          resOptimized = rollup.rollup(rdd, groupingList, attribute, operation)
          resOptimized.count()
        })
        println("--- Time optimized: " + timeOptimized + "ms")
      }

      var correctRes : RDD[(List[Any], Double)] = null
      if (groupingList.length == 3) {
        for (i <- (0 until 5)) {
          val timeBrute = Utils.getTimingInMs(() => {
            val correctRes = df.rollup("lo_orderkey", "lo_linenumber", "lo_partkey").agg(sum("lo_quantity")).rdd
                                      .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1) match { case n: Number => n.doubleValue() }))
            correctRes.count()
          })
          println("--- Time original: " + timeBrute + "ms")
        }
      }

      // val collRes = resOptimized.collect().toMap
      // val collCorRes = correctRes.collect().toMap

      // println("-- res - correctRes, size: " + resOptimized.subtractByKey(correctRes).count)
      // collRes.foreach { case (k, v) => if (collCorRes(k) != v) println("[ERROR] Value difference for key " + k) }
      // println("-- correctRes - res, size: " + correctRes.subtractByKey(resOptimized).count)
      // collCorRes.foreach { case (k, v) => if (collRes(k) != v) println("[ERROR] Value difference for key " + k) }

      // val collResNaive = resNaive.collect().toMap

      // println("-- res - correctRes, size: " + resNaive.subtractByKey(correctRes).count)
      // collResNaive.foreach { case (k, v) => if (collCorRes(k) != v) println("[ERROR] Value difference for key " + k) }
      // println("-- correctRes - res, size: " + correctRes.subtractByKey(resNaive).count)
      // collCorRes.foreach { case (k, v) => if (collResNaive(k) != v) println("[ERROR] Value difference for key " + k) }
    }
  }
}
