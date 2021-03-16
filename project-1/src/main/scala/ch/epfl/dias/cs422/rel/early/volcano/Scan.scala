package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Tuple, PAXPage}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.store._
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import scala.math.ceil

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected val scannable: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  private var data: IndexedSeq[Tuple] = IndexedSeq()
  private var rowIndex = 0
  private var pageIndex = 1

  override def open(): Unit = scannable match {
    case any if (any.getRowCount == 0) =>
      /* There is nothing to do for an empty file */
    case s: ColumnStore =>
      /* Load everything at once */
      data = ((0 until table.getRowType().getFieldCount()) map (i => s.getColumn(i))).transpose
    case s: PAXStore =>
      /* Only load the first page */
      data = s.getPAXPage(0).transpose
    case _ =>
      /* Other cases are handled in next */
  }

  private def incrementAndReturn(row: Tuple): Tuple = {
    rowIndex += 1
    row
  }

  override def next(): Tuple = scannable match {
    case any if (any.getRowCount == 0 || rowIndex >= any.getRowCount) =>
      /* There is nothing left */
      null
    case s: RowStore =>
      incrementAndReturn(s.getRow(rowIndex))
    case s: ColumnStore =>
      incrementAndReturn(data(rowIndex))
    case s: PAXStore if (data.isEmpty) =>
      data = s.getPAXPage(pageIndex).transpose
      pageIndex += 1
      next()
    case s: PAXStore =>
      val row = data.head
      data = data.tail
      rowIndex += 1
      row
  }

  override def close(): Unit = {
    data = IndexedSeq()
    rowIndex = 0
    pageIndex = 1
  }
}
