package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold : Double) extends Construction with Serializable {

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * Near-neighbors are defined as the points with a Jaccard similarity that exceeds the threshold
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * threshold: the similarity threshold that defines near-neighbors
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */

    def jaccardSimilarity(list1: List[String], list2: List[String]): Double =
      (list1 intersect list2).length.toDouble / (list1 union list2).length

    ((rdd cartesian data) filter {
      case ((_, keywordList), (_, dataKeywordList)) => 
        jaccardSimilarity(keywordList, dataKeywordList) > threshold
    })
    .map(x => (x._1._1, x._2._1)).groupByKey
    .map { case (k, v) => (k, v.toSet) }
    
  }
}
