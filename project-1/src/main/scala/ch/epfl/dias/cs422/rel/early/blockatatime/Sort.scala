package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode
import org.apache.calcite.rel.RelFieldCollation
import scala.jdk.CollectionConverters._
import scala.annotation.tailrec

class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {

  private var sortedTuples: IndexedSeq[Tuple] = IndexedSeq()

  @tailrec
  private def gatherTuples(list: IndexedSeq[Tuple]): IndexedSeq[Tuple] = input.next() match {
    case null =>
      list
    case b: Block =>
      gatherTuples(list ++ b)
  }

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

  override def open(): Unit = {
    input.open()
  
    /* We have to know all tuples before doing anything */
    val tuples = gatherTuples(IndexedSeq())
    val fields = collation.getFieldCollations().asScala.toIndexedSeq

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

  override def next(): Block = sortedTuples match {
      case IndexedSeq() =>
        null
      case b =>
        sortedTuples = b drop blockSize
        b take blockSize
    }

  override def close(): Unit =
    input.close()
}
