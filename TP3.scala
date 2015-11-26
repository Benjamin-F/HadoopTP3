package org.src;

object TP3 {
 def main(args: Array[String]) {
  //sc.textfile("/res/spark_assignment/crimes.csv")
  //val filecsv = sc.textFile("/res/spark_assignment/crimes.csv")
  case class Crime(cdatetime: Date, address: String, district: Int, beat: String, grid: Int, crimedescr: String, ucr_ncir_code: Int, latitude: String, longitude: String)
  
  val myCrimes = sc.textFile("/res/spark_assignment/crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  
  //RDD
  val crimes = myCrimes.map(line => { 
   val l = line.split(",") 
   Crime(l(0).toDate, l(1), l(2).toInt, l(3), l(4).toInt, l(5), l(6).toInt, l(7), l(8)) 
  })
  
  //Data frames
  val crimesDF = crimes.toDF
  
  //Q1
  crimesDF.groupBy("crimedescr").count().agg($"crimedescr", max("count")).show // 10851(A)VC TAKE VEH W/O OWNER - 653
  
  //Q2
  

 }
}

