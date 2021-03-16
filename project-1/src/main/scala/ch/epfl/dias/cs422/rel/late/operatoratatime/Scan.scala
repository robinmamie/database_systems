package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple, PAXPage}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ScannableTable, Store}
import ch.epfl.dias.cs422.helpers.store._
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import scala.math.ceil

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {

  private val store = tableToStore(table.unwrap(classOf[ScannableTable]))

  /* Load everything at once */
  private val data = store match {
    case any if (any.getRowCount == 0) =>
      IndexedSeq()
    case s: RowStore  =>
      ((0 until s.getRowCount.toInt) map (i => s.getRow(i)))
    case s: ColumnStore =>
      ((0 until table.getRowType().getFieldCount()) map (i => s.getColumn(i))).transpose
    case s: PAXStore =>
      val firstColumns = s.getPAXPage(0).transpose
      val lastPage = ceil(s.getRowCount.toFloat / firstColumns.size).toInt
      (firstColumns ++ ((1 until lastPage) flatMap (i => s.getPAXPage(i).transpose)))
  }

  override def execute(): IndexedSeq[Column] =
    data.indices map (i => IndexedSeq(i.toLong))

  private lazy val evals =
    new LazyEvaluatorAccess((0 until table.getRowType().getFieldCount()).toList map { i => 
      ((v: Long) => data(v.toInt)(i))
    })

  override def evaluators(): LazyEvaluatorAccess = evals
}
