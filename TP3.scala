//package org.src;
import java.sql.Date;

object TP3 {
 case class Crime(cdatetime: Date, address: String, district: Int, beat: String, grid: Int, crimedescr: String, ucr_ncir_code: Int, latitude: String, longitude: String)

 val myFunc = udf {(x: BigInt) => x.toFloat/30}

 def main(args: Array[String]) {
  //sc.textfile("/res/spark_assignment/crimes.csv")
  //val filecsv = sc.textFile("/res/spark_assignment/crimes.csv")
  val format = new java.text.SimpleDateFormat("dd/MM/yy HH:mm")

  val myCrimes = sc.textFile("/res/spark_assignment/crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

  //RDD
  val crimes = myCrimes.map(line => {
   val l = line.split(",")
   Crime(new java.sql.Date(format.parse(l(0)).getTime()), l(1), l(2).toInt, l(3), l(4).toInt, l(5), l(6).toInt, l(7), l(8))
  })

  //Q1 RDD
  val q1 =  crimes.groupBy( l => l.crimedescr).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(1)
  q1.foreach(println)
  //Q2
  val q2 = crimes.groupBy( l => l.cdatetime).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(3)
  q2.foreach(println)
  //Q3
  val q3 = crimes.groupBy(l => l.crimedescr).map(t => (t._1, t._2.size.toFloat/30))
  q3.foreach(println)

  //Obtenir juste les crimes et leur date:
  // crimes.map(t => (t.crimedescr,t.cdatetime))
  //Data frames
  val crimesDF = crimes.toDF()

  //Q1
  val q1 = crimesDF.groupBy('crimedescr).count().sort(- 'count).take(1)
  q1.foreach(println)

  //Q2
  val q2 = crimesDF.groupBy('cdatetime).count().sort(- 'count).take(3)
  q2.foreach(println)

  //Q3
  val q3 = crimesDF.groupBy('crimedescr).count().select('crimedescr, 'count /30)
  q3.collect.foreach(println)


 }
}
