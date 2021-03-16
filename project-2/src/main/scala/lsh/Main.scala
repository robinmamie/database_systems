package lsh

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import utils._
import org.apache.spark.sql.SparkSession

object Main {

  val threshold = 0.3

  def recall(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double =
    /*
    * Compute the recall for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average recall
    * */
    (ground_truth join lsh_truth)
      .filter { case (k, (a, b)) => a.size > 0}
      .map { case (k, (a, b)) => (a intersect b).size.toDouble / a.size }
      .mean

  def precision(ground_truth : RDD[(String, Set[String])], lsh_truth : RDD[(String, Set[String])]) : Double =
    /*
    * Compute the precision for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average precision
    * */
    (ground_truth join lsh_truth)
      .filter { case (k, (a, b)) => b.size > 0}
      .map { case (k, (a, b)) => (a intersect b).size.toDouble / b.size }
      .mean

  def getConstructionBdcst(sqlContext: SQLContext, rdd_corpus: RDD[(String, List[String])]): Construction =
    new BaseConstructionBroadcast(sqlContext, rdd_corpus)
    // new ANDConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus)))
    // new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus)))
    // new ANDConstruction(List(
    //   new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstructionBroadcast(sqlContext, rdd_corpus)))))

  def getConstruction(sqlContext: SQLContext, rdd_corpus: RDD[(String, List[String])]): Construction =
    new BaseConstruction(sqlContext, rdd_corpus)
    // new ANDConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus)))
    // new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus)))
    // new ANDConstruction(List(
    //   new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus))),
    //   new ORConstruction(List.fill(8)(new BaseConstruction(sqlContext, rdd_corpus)))))

  def query(sc : SparkContext, sqlContext : SQLContext) : Unit = {
    //val corpusSmall = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath
    val rdd_small = sc
      .textFile("/user/cs422/lsh-corpus-small.csv")
      .repartition(16)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
    // val corpusMedium = new File(getClass.getResource("/lsh-corpus-medium.csv").getFile).getPath
    val rdd_medium = sc
      .textFile("/user/cs422/lsh-corpus-medium.csv")
      .repartition(16)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))
    val corpusBig = new File(getClass.getResource("/lsh-corpus-large.csv").getFile).getPath
    val rdd_big = sc
      .textFile("/user/cs422/lsh-corpus-large.csv")
      .repartition(16)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    for {
      queryNb <- (6 until 8)
    } {
      var (rdd_corpus, name) = (rdd_small, "small")
      if (6 <= queryNb) {
        rdd_corpus = rdd_big
        name = "big"
      } else if (3 <= queryNb) {
        rdd_corpus = rdd_medium
        name = "medium"
      }

      println("- Data set " + name + ", query " + queryNb)
      // val query_file = new File(getClass.getResource("/lsh-query-" + queryNb + ".csv").getFile).getPath
      val rdd_query = sc
        .textFile("/user/cs422/lsh-query-" + queryNb + ".csv")
        .map(x => x.toString.split('|'))
        .map(x => (x(0), x.slice(1, x.size).toList))

      val max = if (queryNb < 6) 1 else 0
      var res: RDD[(String, Set[String])] = null
      for (i <- (0 until 20)) {
        var lsh: Construction = null
        val timeLshBdcstInit = Utils.getTimingInMs(() => {
          BaseConstructionBroadcast.reset()
          lsh = getConstructionBdcst(sqlContext, rdd_corpus)
        })
        println("--- Time LSH Broadcast init: " + timeLshBdcstInit + "ms")
        val timeLshBdcst = Utils.getTimingInMs(() => {
          res = lsh.eval(rdd_query)
          res.count()
        })
        println("--- Time LSH Broadcast eval: " + timeLshBdcst + "ms")
      }

      for (i <- (0 until 20)) {
        var lsh: Construction = null
        val timeLshBdcstInit = Utils.getTimingInMs(() => {
          BaseConstruction.reset()
          lsh = getConstruction(sqlContext, rdd_corpus)
        })
        println("--- Time LSH init: " + timeLshBdcstInit + "ms")
        val timeLsh = Utils.getTimingInMs(() => {
          val res = lsh.eval(rdd_query)
          res.count()
        })
        println("--- Time LSH eval: " + timeLsh + "ms")
      }

      var ground: RDD[(String, Set[String])] = null
      for (i <- (0 until max)) {
        val timeBase = Utils.getTimingInMs(() => {
          val exact = new ExactNN(sqlContext, rdd_corpus, threshold)
          val ground = exact.eval(rdd_query)
          ground.count()
        })
        println("--- Time ExactNN: " + timeBase + "ms")
      }
      if (ground != null) {
        // res.saveAsTextFile("/user/group-0/lsh")
        println("-- Recall: " + recall(ground, res))
        println("-- Precision: " + precision(ground, res))
        // assert(recall(ground, res) > 0.83)
        // assert(precision(ground, res) > 0.70)
      }
    }
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-0-lsh")
      //.master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    query(sc, sqlContext)
  }     
}
