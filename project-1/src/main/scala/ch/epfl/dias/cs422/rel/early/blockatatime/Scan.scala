package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Block
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Tuple, PAXPage}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import ch.epfl.dias.cs422.helpers.store._
import scala.math.ceil

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected lazy val store: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  private var data: Block = IndexedSeq()
  private var rowIndex = 0
  private var pageIndex = 1

  override def open(): Unit = store match {
    case any if (any.getRowCount == 0) =>
      /* There is nothing to do for an empty file */
    case s: ColumnStore =>
      /* Load everything at once */
      data = ((0 until table.getRowType().getFieldCount()) map (i => s.getColumn(i))).transpose
    case s: PAXStore =>
      /* Only load the first page */
      data = s.getPAXPage(0).transpose
      rowIndex += data.size
    case _ =>
      /* Other cases are handled in next */
  }

  private def incrementAndReturn(row: Tuple): Tuple = {
    rowIndex += 1
    row
  }

  override def next(): Block = store match {
    case any if (any.getRowCount == 0 || rowIndex >= any.getRowCount) =>
      if (data.isEmpty)
        /* There is nothing left */
        null
      else {
        /* Return last block */
        val block = data
        data = IndexedSeq()
        block
      }
    case s: RowStore if (data.size < blockSize) =>
      /* Load more rows to fill block */
      data = data :+ incrementAndReturn(s.getRow(rowIndex))
      next()
    case _: RowStore =>
      val block = data take blockSize
      data = data drop blockSize
      block
    case s: ColumnStore =>
      rowIndex += blockSize
      val block = data take blockSize
      data = data drop blockSize
      block
    case s: PAXStore if (data.size < blockSize) =>
      /* Load more pages to fill block */
      val newPage = s.getPAXPage(pageIndex).transpose
      data = data ++ newPage
      pageIndex += 1
      rowIndex += newPage.size
      next()
    case s: PAXStore =>
      val block = data take blockSize
      data = data drop blockSize
      block
  }

  override def close(): Unit = {
    data = IndexedSeq()
    rowIndex = 0
    pageIndex = 1
  }
}
