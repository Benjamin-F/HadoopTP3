//package org.src;
import java.sql.Date;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io

object TP3 {
 case class Crime(cdatetime: Date, address: String, district: Int, beat: String, grid: Int, crimedescr: String, ucr_ncir_code: Int, latitude: String, longitude: String)

 def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}

 def main(args: Array[String]) {
  val conf = new SparkConf().setAppName("TP3")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val format = new java.text.SimpleDateFormat("M/dd/yy")

  val myCrimes = sc.textFile("/res/spark_assignment/crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

  //RDD
  val crimes = myCrimes.map(line => {
   val l = line.split(",")
   Crime(new java.sql.Date(format.parse(l(0)).getTime()), l(1), l(2).toInt, l(3), l(4).toInt, l(5), l(6).toInt, l(7), l(8))
  })

  //Q1 RDD
  val rddq1 =  crimes.groupBy( l => l.crimedescr).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(1)
  rddq1.foreach(println)
  //Q2
  val rddq2 = crimes.groupBy( l => l.cdatetime).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(3)
  rddq2.foreach(println)

  //Q3
  val rddq3 = crimes.groupBy(l => l.crimedescr).map(t => (t._1, t._2.size.toFloat/30))
  rddq3.foreach(println)

  //Obtenir juste les crimes et leur date:
  // crimes.map(t => (t.crimedescr,t.cdatetime))
  //Data frames
  val crimesDF = crimes.toDF()

  //Q1
  val dfq1 = crimesDF.groupBy('crimedescr).count().sort(- 'count).take(1)
  dfq1.foreach(println)

  //Q2
  val dfq2 = crimesDF.groupBy('cdatetime).count().sort(- 'count).take(3)
  dfq2.foreach(println)

  //Q3
  val dfq3 = crimesDF.groupBy('crimedescr).count().select('crimedescr, 'count /31)
  dfq3.collect.foreach(println)

 //PART2
 val part2 = crimesDF.groupBy('district).count.select('district, 'count /31 as "Average")
 part2.collect.foreach(println)

 printToFile(new java.io.File("AverageNumberOfCrimePerDayPerDistrict.csv")) { p =>
  part2.collect.foreach(l => p.println(l(0)+","+l(1)))
}

 }
}
