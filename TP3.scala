//package org.src;
import java.util.Date;

object TP3 {
 case class Crime(cdatetime: Date, address: String, district: Int, beat: String, grid: Int, crimedescr: String, ucr_ncir_code: Int, latitude: String, longitude: String)

 def main(args: Array[String]) {
  //sc.textfile("/res/spark_assignment/crimes.csv")
  //val filecsv = sc.textFile("/res/spark_assignment/crimes.csv")
  val format = new java.text.SimpleDateFormat("dd/MM/yy HH:mm")

  val myCrimes = sc.textFile("/res/spark_assignment/crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

  //RDD
  val crimes = myCrimes.map(line => {
   val l = line.split(",")
   Crime(format.parse(l(0)), l(1), l(2).toInt, l(3), l(4).toInt, l(5), l(6).toInt, l(7), l(8))
  })

  //Q1 RDD
  val q1 =  crimes.groupBy( l => l.crimedescr).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(1)
  q1.foreach(println)
  //Q2
  val q2 = crimes.groupBy( l => l.crimedescr).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(3)
  q2.foreach(println)
  //Q3
  val q3 = crimes.map(t => (t.crimedescr,t.cdatetime)).groupBy(l => l._1).map(t => (t._1, t._2.size))// a diviser par 31, nombre de jours en Janvier
  q3.foreach(println)

  //Data frames
  val crimesDF = crimes.toDF()

  //Q1
  crimesDF.groupBy("crimedescr").count().agg($"crimedescr", max("count")).show // 10851(A)VC TAKE VEH W/O OWNER - 653
  crimesDF.groupBy("crimedescr").agg($"crimedescr", max(crimesDF.groupBy("crimedescr").count()).show

  //Q2


 }
}
