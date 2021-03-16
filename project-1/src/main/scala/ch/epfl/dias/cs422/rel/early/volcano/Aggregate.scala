package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import scala.collection.JavaConverters._
import scala.annotation.tailrec

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  private var aggregates: List[Tuple] = Nil
  private val emptyAggs = (aggCalls map aggEmptyValue).toIndexedSeq

  @tailrec
  private def gatherTuples(list: List[Tuple]): List[Tuple] = input.next() match {
    case null =>
      list.reverse
    case t: Tuple =>
      gatherTuples(t :: list)
  }

  override def open(): Unit = {
    input.open()
    /* We have to know all tuples before doing anything */
    val tuples = gatherTuples(Nil)

    if (tuples.isEmpty)
      /* Return empty aggregates */
      aggregates = List(emptyAggs)
    else {
      /* First, group by the given keys */
      val scalaGroupSets = groupSet.asScala.toList
      val groups = tuples groupBy { t => (scalaGroupSets map { i => t(i) }).toIndexedSeq.asInstanceOf[Tuple] }

      /* Aggregate values */
      val results = (groups.values foldLeft (Nil: List[Tuple])) { (acc1, head) =>
        /* Per group... */
        ((head foldLeft emptyAggs) { (acc2, curr) =>
          /* ... per tuple... */
          (acc2 zip aggCalls) map { case (prev, f) =>
            /* ... per aggregate */
            if (prev == null)
              /* Monoid: get first element as base */
              f.getArgument(curr)
            else
              aggReduce(prev, f.getArgument(curr), f)
          }
        }) :: acc1
      }
      aggregates = ((groups.keys zip results.reverse) map { case (a, b) => a ++ b }).toList
    }
  }

  override def next(): Tuple = aggregates match {
    case Nil =>
      null
    case head :: tail =>
      aggregates = tail
      head
  } 

  override def close(): Unit =
    input.close()
}
