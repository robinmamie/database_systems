package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorRoot
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val leftVIDs = left.execute()
    val rightVIDs = right.execute()

    /* Only materialise what is strictly necessary */
    lazy val leftEval = indices(getLeftKeys, left.getRowType(), left.evaluators())
    lazy val rightEval = indices(getRightKeys, right.getRowType(), right.evaluators())

    /* Group tuples by left keys */
    val leftTuples = (leftVIDs zip (leftVIDs map { v => leftEval(v) })) groupBy (_._2)
    val rightTuples = rightVIDs zip (rightVIDs map { v => rightEval(v) })

    /* Join inputs using these groups */
    rightTuples flatMap { rt =>
      leftTuples getOrElse (rt._2, Nil) map (lt => IndexedSeq(lt._1, rt._1))
    }
  }

  private lazy val evals =
    lazyEval(left.evaluators(), right.evaluators(), left.getRowType, right.getRowType)

  override def evaluators(): LazyEvaluatorRoot = evals
}
