package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import scala.jdk.CollectionConverters._
import org.apache.calcite.util.ImmutableBitSet

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  private val emptyAggs = (aggCalls map aggEmptyValue).toIndexedSeq

  override def execute(): IndexedSeq[Column] =
    data.indices map(i => IndexedSeq(i.toLong))

  private val data: IndexedSeq[Tuple] = {
    val tupleVIDs = input.execute().toIndexedSeq
    if (tupleVIDs.isEmpty)
      /* Return empty aggregates */
      emptyAggs.map(IndexedSeq(_)).toIndexedSeq
    else {
      /* First, group by the given keys, only materialising what is strictly
         necessary */
      val scalaGroupSets = groupSet.asScala.toIndexedSeq
      /* Small trick to get the arguments' indices */
      val fakeTuple = (2 until input.getRowType().getFieldCount()+2).toIndexedSeq.asInstanceOf[Tuple]
      val aggIndices = aggCalls.toIndexedSeq map { agg => agg.getArgument(fakeTuple).asInstanceOf[Int] - 2 }
      lazy val indicesEval = indices(scalaGroupSets.asInstanceOf[IndexedSeq[Int]] ++ aggIndices.filter(_ >= 0), input.getRowType(), input.evaluators())
      val tuples = tupleVIDs map { v => indicesEval(v) }
      val groups = tuples groupBy { t => (scalaGroupSets.indices map { i => t(i) }).toIndexedSeq.asInstanceOf[Tuple] }

      /* Aggregate values */
      val results = (groups.values foldLeft (IndexedSeq(): IndexedSeq[Tuple])) { (acc1, head) =>
        /* Per group... */
        acc1 ++ IndexedSeq((head foldLeft emptyAggs) { (acc2, curr) =>
          /* ... per tuple... */
          (acc2 zip (aggCalls zip aggIndices)) map { case (prev, (f, ind)) =>
            /* ... per aggregate */
            if (prev == null)
              /* Monoid: get first element as base */
              f.getArgument(curr)
            else if (ind == -1)
              aggReduce(prev, 1, f)
            else
              aggReduce(prev, curr(ind), f)
          }
        })
      }
      ((groups.keys zip results) map { case (a, b) => a ++ b }).toIndexedSeq
    }
  }

  private lazy val evals =
    new LazyEvaluatorAccess(data.head.indices.toList map { i => 
      ((v: Long) => data(v.toInt)(i))
    })

  override def evaluators(): LazyEvaluatorAccess = evals
}
