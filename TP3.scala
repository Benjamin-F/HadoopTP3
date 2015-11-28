//package org.src;
import java.sql.Date;

object TP3 {
 case class Crime(cdatetime: Date, address: String, district: Int, beat: String, grid: Int, crimedescr: String, ucr_ncir_code: Int, latitude: String, longitude: String)

 def main(args: Array[String]) {
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
 val part2 = crimesDF.groupBy('district).count.select('district, 'count /31)

 part2.write.format("com.databricks.sparl.csv").option("header","true").save("average of crime per district per day.csv")

 }
}
