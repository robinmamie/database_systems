package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Column
import ch.epfl.dias.cs422.helpers.rel.late.{Evaluator, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {

  lazy val e: Evaluator =
    eval(condition, input.getRowType, input.evaluators())

  override def execute(): IndexedSeq[Column] =
    input.execute() filter { v => e(v) match { case b: Boolean => b } }

  override def evaluators(): LazyEvaluatorRoot =
    input.evaluators()
}
