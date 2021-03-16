package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorRoot
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import org.apache.calcite.rel.RelFieldCollation

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  @tailrec
  private def getCompareToValue(fields: IndexedSeq[RelFieldCollation],
                                t1: Tuple,
                                t2: Tuple): Boolean = fields match {
    case IndexedSeq() => 
      /* Does not matter */
      true
    case _ =>
      (t1.head, t2.head) match {
        case (c1: Comparable[Any], c2: Comparable[Any]) =>
          val compareValue = c1 compareTo c2
          /* Either continue or stop if there is a difference */
          if (compareValue != 0)
            fields.head.direction.isDescending == (compareValue > 0)
          else
            getCompareToValue(fields.tail, t1.tail, t2.tail)
      }
  }

  override def execute(): IndexedSeq[Column] = {
    val tupleVIDs = input.execute()
    val fields = collation.getFieldCollations().asScala.toIndexedSeq

    /* Only materialise what is strictly necessary */
    val fieldIndices = fields.toIndexedSeq map { f => f.getFieldIndex() }
    lazy val indicesEval = indices(fieldIndices, input.getRowType(), input.evaluators())
    val tuples = tupleVIDs zip (tupleVIDs map { v => indicesEval(v) })

    /* Sort tuples */
    var sortedTuples = tuples sortWith { case (t1, t2) =>
      getCompareToValue(fields, t1._2, t2._2)
    }

    /* Drop the first offset tuples */
    if (offset != null)
      evalLiteral(offset) match {
        case i: Int =>
          sortedTuples = sortedTuples drop i
      }

    /* Keep only the next fetch tuples */
    if (fetch != null)
      evalLiteral(fetch) match {
        case i: Int =>
          sortedTuples = sortedTuples take i
      }
    sortedTuples.map(_._1)
  }

  override def evaluators(): LazyEvaluatorRoot =
    input.evaluators()
}
