package lsh
import org.apache.spark.rdd.RDD

class ANDConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * Implement an ANDConstruction for one or more LSH constructions (simple or composite)
    * children: LSH constructions to compose
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */
    children map { c => c.eval(rdd) } reduce { (a, b) => a union b } reduceByKey {
      (a, b) =>
        a intersect b
    }
  }
}
