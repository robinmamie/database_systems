package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.util.Random

object BaseConstructionBroadcast {
  val baseSeed = 42
  private var seed = baseSeed
  def getAndIncrementSeed(): Int = {
    val toReturn = seed
    seed += 1
    toReturn
  }
  def reset() =
    seed = baseSeed
}

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction with Serializable {
  /*
  * Initialize LSH data structures here
  * You need to broadcast the data structures to all executors and use them locally
  * */
  val r = new Random(BaseConstructionBroadcast.getAndIncrementSeed())
  val perturbation = r.nextString(21)

  def getSignature(rdd:RDD[(String, List[String])]) :RDD[(Int, String)] = {
    rdd map { case (title, keywords) => 
      (keywords.map(keyword => (keyword + perturbation).hashCode).min , title)
    }
  }
  val dataSignature =
    sqlContext.sparkContext
      .broadcast(
        getSignature(data)
          .mapValues { title => Set(title) }
          .reduceByKey { case (title1, title2) => title1 ++ title2 }
          .collect.toMap
      )

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * You need to perform the queries by using LSH with min-hash.
    * The perturbations needs to be consistent - decided once and randomly for each BaseConstructor object
    * sqlContext: current SQLContext
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */
    getSignature(rdd) map { case (keyword, movie) =>
      (movie, dataSignature.value getOrElse (keyword, Set()))
    }
  }
}
