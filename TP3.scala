//package org.src;
import java.util.Date;

object TP3 {
 case class Crime(cdatetime: Date, address: String, district: Int, beat: String, grid: Int, crimedescr: String, ucr_ncir_code: Int, latitude: String, longitude: String)
 case class CrimeDF(cdatetime: String, address: String, district: Int, beat: String, grid: Int, crimedescr: String, ucr_ncir_code: Int, latitude: String, longitude: String)

 def main(args: Array[String]) {
  //sc.textfile("/res/spark_assignment/crimes.csv")
  //val filecsv = sc.textFile("/res/spark_assignment/crimes.csv")
  val format = new java.text.SimpleDateFormat("M/dd/yy")

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
  val q2 = crimes.groupBy( l => l.cdatetime).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(3)
  q2.foreach(println)
  //Q3: Taille du fichier crimes diviser par le nbr de jours en janvier
  val q3 = crimes.count()/31
  q3.foreach(println)

  //Data frames
  val crimesDF = crimes.toDF()

  //Q1
  crimesDF.groupBy("crimedescr").count().agg($"crimedescr", max("count")).show // 10851(A)VC TAKE VEH W/O OWNER - 653
  crimesDF.groupBy("crimedescr").agg($"crimedescr", max(crimesDF.groupBy("crimedescr").count()).show //Marche pas

  //Q2


 }
}
