package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
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
      val key = fields.head.getFieldIndex()
      (t1(key), t2(key)) match {
        case (c1: Comparable[Any], c2: Comparable[Any]) =>
          val compareValue = c1 compareTo c2
          /* Either continue or stop if there is a difference */
          if (compareValue != 0)
            fields.head.direction.isDescending == (compareValue > 0)
          else
            getCompareToValue(fields.tail, t1, t2)
      }
  }

  override def execute(): IndexedSeq[Column] =  {
    val tuples = input.execute().transpose
    val fields = collation.getFieldCollations().asScala.toIndexedSeq

    /* Sort tuples */
    var sortedTuples = tuples sortWith { case (t1, t2) =>
      getCompareToValue(fields, t1, t2)
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
    sortedTuples.transpose
  }
}
