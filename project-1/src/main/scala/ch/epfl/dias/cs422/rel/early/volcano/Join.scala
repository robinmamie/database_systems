package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode
import scala.annotation.tailrec

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  private var joinedTuples: List[Tuple] = Nil

  @tailrec
  private def gatherTuples(input: Operator,
                           l: List[Tuple]): List[Tuple] = input.next() match {
    case null =>
      l.reverse
    case t: Tuple =>
      gatherTuples(input, t :: l)
  }

  override def open(): Unit = {
    /* We have to know all tuples before doing anything */
    left.open()
    right.open()

    /* Group tuples by left keys */
    val leftTuples = gatherTuples(left, Nil) groupBy (getLeftKeys map _)

    /* Join inputs using these groups */
    joinedTuples = gatherTuples(right, Nil) flatMap { rt =>
      leftTuples getOrElse (getRightKeys map rt, Nil) map (_ ++ rt)
    }
  }

  override def next(): Tuple = joinedTuples match {
    case Nil =>
      null
    case head :: tail =>
      joinedTuples = tail
      head
  }

  override def close(): Unit = {
    left.close()
    right.close()
  }
}
