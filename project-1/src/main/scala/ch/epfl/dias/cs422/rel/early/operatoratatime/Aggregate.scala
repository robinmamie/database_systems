package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import scala.jdk.CollectionConverters._
import scala.annotation.tailrec

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  private val emptyAggs = (aggCalls map aggEmptyValue).toIndexedSeq

  override def execute(): IndexedSeq[Column] = {
    val tuples = input.execute().transpose.toIndexedSeq
    if (tuples.isEmpty) {
      /* Return empty aggregates */
      IndexedSeq(emptyAggs).transpose
    } else {
      /* First, group by the given keys */
      val scalaGroupSets = groupSet.asScala.toIndexedSeq
      val groups = tuples groupBy { t => (scalaGroupSets map { i => t(i) }).toIndexedSeq.asInstanceOf[Tuple] }

      /* Aggregate values */
      val results = (groups.values foldLeft (IndexedSeq(): IndexedSeq[Tuple])) { (acc1, head) =>
        /* Per group... */
        acc1 ++ IndexedSeq((head foldLeft emptyAggs) { (acc2, curr) =>
          /* ... per tuple... */
          (acc2 zip aggCalls) map { case (prev, f) =>
            /* ... per aggregate */
            if (prev == null)
              /* Monoid: get first element as base */
              f.getArgument(curr)
            else
              aggReduce(prev, f.getArgument(curr), f)
          }
        })
      }
      ((groups.keys zip results) map { case (a, b) => a ++ b }).toIndexedSeq.transpose
    }
  }
}
