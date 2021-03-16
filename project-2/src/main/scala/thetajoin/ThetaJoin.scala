package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.util.Random.nextInt

import org.slf4j.LoggerFactory
import org.apache.spark.HashPartitioner

class ThetaJoin(partitions: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("ThetaJoin")

  /*
  this method takes as input two datasets (dat1, dat2) and returns the pairs of join keys that satisfy the theta join condition.
  attrIndex1: is the index of the join key of dat1
  attrIndex2: is the index of the join key of dat2
  condition: Is the join condition. Assume only "<", ">" will be given as input
  Assume condition only on integer fields.
  Returns and RDD[(Int, Int)]: projects only the join keys.
   */
  def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition:String): RDD[(Int, Int)] = {
    val sSize = dat1.count()
    val rSize = dat2.count()
    val blockSize = sSize * rSize / partitions
    val denom = math.sqrt(blockSize)
    val cs = (sSize / denom).toInt
    val cr = (rSize / denom).toInt

    val partitioner = new HashPartitioner(partitions)

    val sSample = dat1
                  .takeSample(false, cs - 1, 42)
                  .map(r => r getInt attrIndex1)
    val rSample = dat2
                  .takeSample(false, cr - 1, 42)
                  .map(r => r getInt attrIndex2)
    val partS = dat1
                  .flatMap {r =>
                    val el = r getInt attrIndex1
                    val colBucket = sSample count { x => x < el }
                    (0 until cs) map (i => (colBucket + i*cr, r))
                  }
                  .partitionBy(partitioner)
    val partR = dat2
                  .flatMap {r =>
                    val el = r.getInt(attrIndex2)
                    val rowBucket = rSample count { x => x < el }
                    (0 until cr) map (i => (i + rowBucket, r))
                  }
                  .partitionBy(partitioner)

    (partS join partR)
      .map {
        case (k, (s, r)) =>
          (s getInt attrIndex1, r getInt attrIndex2)
      }
      .filter { case (s, r) => condition match {
        case ">" =>
          s > r
        case "<" =>
          s < r
      }}
  }
}
