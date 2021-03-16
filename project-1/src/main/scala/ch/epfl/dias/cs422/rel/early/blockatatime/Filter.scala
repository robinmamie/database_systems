package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {

  private var block: Block = IndexedSeq()
  lazy val e: Tuple => Any =
    eval(condition, input.getRowType)

  override def open(): Unit =
    input.open()

  override def next(): Block =  block match {
    case b: Block if b.size < blockSize => input.next() match {
      case null =>
        block = b drop blockSize
        if (b.nonEmpty) b take blockSize else null
      case newB =>
        /* Join inputs using the left groups */
        block = block ++ (newB filter (e(_) match { case b: Boolean => b }))
        next()
    }
    case b: Block => 
      block = b drop blockSize
      b take blockSize
  }

  override def close(): Unit = {
    block = IndexedSeq()
    input.close()
  }
}
