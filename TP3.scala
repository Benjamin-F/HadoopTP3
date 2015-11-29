//package org.src;
import java.sql.Date;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io

object TP3 {
  //Declaration of a class fitting the datas registered in the crimes.csv file.
 case class Crime(cdatetime: Date, address: String, district: Int, beat: String, grid: Int, crimedescr: String, ucr_ncir_code: Int, latitude: String, longitude: String)

//Simple function used to write in a file specified on input anykind of information
//It will be used to save in a file the output of the last request with csv format
 def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}

 def main(args: Array[String]) {
   //Basic configuration
  val conf = new SparkConf().setAppName("TP3")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  //Declaring a date format to fit the date format in crimes.csv
  val format = new java.text.SimpleDateFormat("M/dd/yy")

  //Loading crimes.csv
  val myCrimes = sc.textFile("/res/spark_assignment/crimes.csv").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

  //Spliting the lines the read file to fill instance of crime class. RDD is instanciated
  val crimes = myCrimes.map(line => {
   val l = line.split(",")
   Crime(new java.sql.Date(format.parse(l(0)).getTime()), l(1), l(2).toInt, l(3), l(4).toInt, l(5), l(6).toInt, l(7), l(8))
  })

  /***********Requests using RDD***********/
  //Q1 : What is the crime that happens the most in Sacramento ?
  //Grouping by crime and counting number of crimes. Result is order by decreasing order of crime number. The first result is returned
  val rddq1 =  crimes.groupBy( l => l.crimedescr).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(1)
  rddq1.foreach(println)

  //Q2 : Give the 3 days with the highest crime count
  //Grouping by day and counting number of crime. Result is order by decreasing order of crime number. The first three number are returned
  val rddq2 = crimes.groupBy( l => l.cdatetime).map(t => (t._1, t._2.size)).sortBy(u => - u._2).take(3)
  rddq2.foreach(println)

  //Q3 : Calculate the average of each crime per day
  //Grouping by crime, counting number of crimes and dividing by 31, number of days in January.
  val rddq3 = crimes.groupBy(l => l.crimedescr).map(t => (t._1, t._2.size.toFloat/30))
  rddq3.foreach(println)

  //Obtenir juste les crimes et leur date:
  // crimes.map(t => (t.crimedescr,t.cdatetime))

  /***********Requests using Dataframe***********/
  //The same requests as RDD are computed using dataframe methods.

  //Instanciate a Dataframe
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

 /***********PART2***********/
 //Average of crimes per day per districts
 //Grouping by district, counting number of crime and dividing by 31, number of day in January
 val part2 = crimesDF.groupBy('district).count.select('district, 'count /31 as "Average")
 part2.collect.foreach(println)

 //Using home made file writing function to create csv file.
 printToFile(new java.io.File("AverageNumberOfCrimePerDayPerDistrict.csv")) { p =>
  part2.collect.foreach(l => p.println(l(0)+","+l(1)))
}

 }
}
