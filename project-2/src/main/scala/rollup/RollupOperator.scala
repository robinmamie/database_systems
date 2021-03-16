package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.math.min
import scala.math.max

class RollupOperator() extends java.io.Serializable {

  /*
 * This method gets as input one dataset, the indexes of the grouping attributes of the rollup (ROLLUP clause)
 * the index of the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = List[Any], value = Double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def reduceOp(op: String, a: Double, aCount: Int, b: Double, bCount: Int): (Double, Int) = op match {
    case "AVG" =>
      (a + b, aCount + bCount)
    case "COUNT" | "SUM" =>
      (a + b, 0)
    case "MIN" =>
      (min(a, b), 0)
    case "MAX" =>
      (max(a, b), 0)
  }

  def rollup(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    (groupingAttributeIndexes.indices.reverse foldLeft {
      val base =
        dataset
          .map(r => (groupingAttributeIndexes map (i => r get i), (agg match {
              case "COUNT" =>
                1.0d
              case _ =>
                r getInt aggAttributeIndex
            }, 1)))
          .reduceByKey { case ((a, aCount), (b, bCount)) =>
            reduceOp(agg, a, aCount, b, bCount)
          }
      (base, base)
    }) {
      case ((aggDataset, prevDataset), size) =>
        val current =
          prevDataset
            .map { case (k, v) => (k dropRight 1, v) }
            .reduceByKey { case ((a, aCount), (b, bCount)) =>
              reduceOp(agg, a, aCount, b, bCount)
            }
        (aggDataset union current, current)
    }
    ._1.map { case (key, (value, valCount)) =>
      if (agg == "AVG")
        (key, value / valCount)
      else
        (key, value)
    }
  }

  def rollup_naive(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] =
    groupingAttributeIndexes
      .scanLeft(Nil: List[Int])(_ :+ _)
      .map { indices =>
        dataset
          .map(r => (indices map (i => r get i), (agg match {
            case "COUNT" =>
              1.0d
            case _ =>
              r getInt aggAttributeIndex
          }, 1)))
          .reduceByKey { case ((a, aCount), (b, bCount)) =>
          reduceOp(agg, a, aCount, b, bCount)
        }
      }
      .reduce(_ union _)
      .map { case (key, (value, valCount)) =>
        if (agg == "AVG")
          (key, value / valCount)
        else
          (key, value)
      }
}
