package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import scala.annotation.tailrec

import scala.jdk.CollectionConverters._

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  private var aggregates: IndexedSeq[Tuple] = IndexedSeq()
  private val emptyAggs = (aggCalls map aggEmptyValue).toIndexedSeq

  @tailrec
  private def gatherTuples(list: IndexedSeq[Tuple]): IndexedSeq[Tuple] = input.next() match {
    case null =>
      list
    case b: Block =>
      gatherTuples(list ++ b)
  }

  override def open(): Unit = {
    input.open()
    /* We have to know all tuples before doing anything */
    val tuples = gatherTuples(IndexedSeq())

    if (tuples.isEmpty)
      /* Return empty aggregates */
      aggregates = IndexedSeq(emptyAggs)
    else {
      /* First, group by the given keys */
      val scalaGroupSets = groupSet.asScala.toList
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
      aggregates = ((groups.keys zip results) map { case (a, b) => a ++ b }).toIndexedSeq
    }
  }

  override def next(): Block = aggregates match {
    case IndexedSeq() =>
      null
    case blocks =>
      aggregates = blocks drop blockSize
      (blocks take blockSize).toIndexedSeq
  }

  override def close(): Unit =
    input.close()
}
