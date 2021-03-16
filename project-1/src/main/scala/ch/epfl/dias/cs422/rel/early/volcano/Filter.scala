package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {

  lazy val e: Tuple => Any =
    eval(condition, input.getRowType)

  override def open(): Unit =
    input.open()

  override def next(): Tuple = input.next() match {
    case null =>
      null
    case tuple: Tuple =>
      e(tuple) match {
        case false =>
          /* Condition not fulfilled */
          next()
        case true =>
          /* Condition fulfilled */
          tuple
      }
  }

  override def close(): Unit =
    input.close()
}
