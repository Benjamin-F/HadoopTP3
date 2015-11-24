package org.src;

object TP3 {
 def main(args: Array[String]) {
  sc.textfile("/res/spark_assignment/crimes.csv")
  val filecsv = sc.textFile("/res/spark_assignment/crimes.csv")
  case class Crime(cdatetime: String, address: String, district: int, beat: String, grid: int, crimedescr: String, ucr_ncir_code: int, latitude: double, longitude: double)
 }
}
