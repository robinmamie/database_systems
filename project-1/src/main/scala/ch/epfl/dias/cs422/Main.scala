package ch.epfl.dias.cs422

import ch.epfl.dias.cs422.helpers.builder.Factories
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.{PrintUtil, SqlPrepare}

object Main {
  def main(args: Array[String]): Unit = {
    val sql = """
select * from empty_table
      """

    val prep = SqlPrepare(Factories.LAZY_OPERATOR_AT_A_TIME_INSTANCE, "rowstore")
    val rel = prep.prepare(sql)

    PrintUtil.printTree(rel)

    for (i <- 1 to 10) {
      println("Iteration " + i + " :")
      rel.asInstanceOf[Operator].foreach(println)
//      // equivalent:
//      rel.open()
//      breakable {
//        while (true) {
//          val n = rel.next()
//          if (n == Nil) break // It's not safe to call next again after it returns Nil
//          println(n)
//        }
//      }
//      rel.close()
    }
  }
}