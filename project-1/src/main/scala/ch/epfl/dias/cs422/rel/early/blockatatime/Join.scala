package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode
import scala.annotation.tailrec

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  private var leftTuples: Map[Tuple, IndexedSeq[Tuple]] = Map.empty
  private var block: Block = IndexedSeq()

  @tailrec
  private def gatherTuples(input: Operator,
                           l: IndexedSeq[Tuple]): IndexedSeq[Tuple] = input.next() match {
    case null =>
      l
    case b: Block =>
      gatherTuples(input, l ++ b)
  }

  override def open(): Unit = {
    /* We have to know all tuples before doing anything */
    left.open()
    right.open()

    /* Group tuples by left keys */
    leftTuples = gatherTuples(left, IndexedSeq()) groupBy (getLeftKeys map _)
  }

  override def next(): Block = block match {
    case b: Block if b.size < blockSize => right.next() match {
      case null =>
        block = b drop blockSize
        if (b.nonEmpty) b take blockSize else null
      case rightB =>
        /* Join inputs using the left groups */
        block = block ++ (rightB flatMap { rt =>
          leftTuples getOrElse (getRightKeys map rt, Nil) map (_ ++ rt)
        })
        next()
    }
    case b: Block => 
      block = b drop blockSize
      b take blockSize
  }

  override def close(): Unit = {
    left.close()
    right.close()
    leftTuples = Map.empty
    block = IndexedSeq()
  }
}
