package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import org.apache.calcite.rel.RelFieldCollation

class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  private var sortedTuples: List[Tuple] = Nil

  @tailrec
  private def gatherTuples(l: List[Tuple]): List[Tuple] = input.next() match {
    case null =>
      l.reverse
    case t: Tuple =>
      gatherTuples(t :: l)
  }

  @tailrec
  private def getCompareToValue(fields: List[RelFieldCollation],
                                t1: Tuple,
                                t2: Tuple): Boolean = fields match {
    case Nil => 
      /* Does not matter */
      true
    case head :: tail =>
      val key = head.getFieldIndex()
      (t1(key), t2(key)) match {
        case (c1: Comparable[Any], c2: Comparable[Any]) =>
          val compareValue = c1 compareTo c2
          /* Either continue or stop if there is a difference */
          if (compareValue != 0)
            head.direction.isDescending == (compareValue > 0)
          else
            getCompareToValue(tail, t1, t2)
      }
  }

  override def open(): Unit = {
    input.open()

    /* We have to know all tuples before doing anything */
    val tuples = gatherTuples(Nil)
    val fields = collation.getFieldCollations().asScala.toList

    /* Sort tuples */
    sortedTuples = tuples sortWith { case (t1, t2) =>
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
  }

  override def next(): Tuple = sortedTuples match {
    case Nil =>
      null
    case head :: tail =>
      sortedTuples = tail
      head
  }

  override def close(): Unit =
    input.close()
}
