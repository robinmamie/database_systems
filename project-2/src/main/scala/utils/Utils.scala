package utils

object Utils {
  def getTimingInMs(toLaunch: () => Unit): Double = {
    val t0 = System.nanoTime()
    toLaunch()
    val t1 = System.nanoTime()
    (t1 - t0) / 1e6
  }
}