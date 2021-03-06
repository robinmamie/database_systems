package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Tuple, Column}
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {

  private lazy val e: Tuple => Any =
    eval(condition, input.getRowType)

  override def execute(): IndexedSeq[Column] =
    (input.execute().transpose filter { t => e(t) match { case b: Boolean => b } }).transpose
}
