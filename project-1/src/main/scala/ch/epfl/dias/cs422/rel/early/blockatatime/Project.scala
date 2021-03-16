package ch.epfl.dias.cs422.rel.early.blockatatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

class Project protected (input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {

  lazy val evaluator: Tuple => Tuple =
    eval(projects.asScala.toIndexedSeq, input.getRowType)

  override def open(): Unit = input.open()

  override def next(): Block = input.next() match {
    case null =>
      null
    case b: Block =>
      /* Simply project the next rows */
      b map evaluator
  }

  override def close(): Unit = input.close()
}
