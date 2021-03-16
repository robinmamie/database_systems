package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, Row}

import utils._
import java.io._

object Main {

  def loadRDD(sqlContext: SQLContext, file: String): RDD[Row] = {
    //val input = new File(getClass.getResource(file).getFile).getPath
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(file).rdd
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-0-thetajoin")
      //.master("local[*]")
      .getOrCreate()

    val attrIndex1 = 2
    val attrIndex2 = 2

    val rddA = loadRDD(spark.sqlContext, "/user/cs422/taxA50K.csv")
    val rddB = loadRDD(spark.sqlContext, "/user/cs422/taxB50K.csv")

    for (size <- List(1000, 2000, 4000, 8000, 16000, 32000, 50000)) {
      val rddACurr = spark.sparkContext.makeRDD(rddA.take(size))
      val rddBCurr = spark.sparkContext.makeRDD(rddB.take(size))

      for (i <- List(1, 4, 16, 25, 100, 1000, 4000, 20000, 40000, 100000)) {
        for (j <- (0 until 5)) {
          val time = Utils.getTimingInMs(() => {
            val thetaJoin = new ThetaJoin(i)
            val res = thetaJoin.ineq_join(rddACurr, rddBCurr, attrIndex1, attrIndex2, "<")
            res.count()
          })
          println("--- data size " + rddACurr.count() + ", part. size " + i + ": " + time + "ms")
        }
      }
      for (j <- (0 until 5)) {
        val time = Utils.getTimingInMs(() => {
          val cartesianRes = rddACurr.cartesian(rddBCurr)
                              .filter(x => x._1(attrIndex1).asInstanceOf[Int] < x._2(attrIndex2).asInstanceOf[Int])
                              .map(x => (x._1(attrIndex1).asInstanceOf[Int], x._2(attrIndex2).asInstanceOf[Int]))
          cartesianRes.count()
        })
        println("--- cartesian, data size " + rddACurr.count() + ": " + time + "ms")
      }
    }
  }
}
