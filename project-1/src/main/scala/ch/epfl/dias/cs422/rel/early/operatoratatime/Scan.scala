package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Tuple, Column, PAXPage}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import ch.epfl.dias.cs422.helpers.store._
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import scala.math.ceil

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {

  override def execute(): IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))

    /* Load everything at once */
    store match {
      case _ if (store.getRowCount == 0) =>
        IndexedSeq()
      case s: RowStore =>
        ((0 until s.getRowCount.toInt) map (i => s.getRow(i))).transpose
      case s: ColumnStore =>
        ((0 until table.getRowType().getFieldCount()) map (i => s.getColumn(i)))
      case s: PAXStore =>
        val firstColumns = s.getPAXPage(0).transpose
        val lastPage = ceil(s.getRowCount.toFloat / firstColumns.size).toInt
        (firstColumns ++ ((1 until lastPage) flatMap (i => s.getPAXPage(i).transpose))).transpose
    }
  }
}
