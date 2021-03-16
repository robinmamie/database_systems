package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rex.RexNode
import scala.annotation.tailrec

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  override def execute(): IndexedSeq[Column] = {
      /* Group tuples by left keys */
      val leftTuples = (left.execute().transpose map { t => t.asInstanceOf[Tuple] }) groupBy (getLeftKeys map _)
      val rightTuples = (right.execute().transpose map { t => t.asInstanceOf[Tuple] })

      /* Join inputs using these groups */
      (rightTuples flatMap { rt =>
        leftTuples getOrElse (getRightKeys map rt, Nil) map (_ ++ rt)
      }).transpose
  }
}
