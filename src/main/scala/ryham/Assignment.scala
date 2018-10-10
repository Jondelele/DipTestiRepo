package ryham

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col}

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType, DoubleType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg}
import org.apache.spark.sql.functions.unix_timestamp

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import com.databricks.spark.xml._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.ml.regression.LinearRegressionModel

import java.lang.Thread
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Assignment extends App {
  println("Assignment Main Starting ----------------------------------------")
  
  def task1() = {
  	// X and Y coordinate columns are taken from trafficAccidentDataFrame df
    val data: DataFrame = trafficAccidentDataFrame.select("X","Y")
    
    import org.apache.spark.ml.feature.VectorAssembler
    
		val vectorAssembler = new VectorAssembler()
		.setInputCols(Array("X", "Y"))
		.setOutputCol("features")

    
//		myDf.show()
   }
  
  def task2() = {
      println("Task2 starting")
   }
  
  def task3() = {
      println("Task3 starting")
   }
  
  def task4() = {
      println("Task4 starting")
   }

  def task5() = {
      println("Task5 starting")
   }
  
  def task6() = {
      println("Task6 starting")
   }
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
                          .appName("ryhamMain")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
          
  val  trafficAccidentDataFrame: DataFrame = spark.read
                                           .format("csv")
                                           .option("sep", ";")
                                           .option("header", "true")
                                           .option("inferSchema", "true")
                                           .load("data/tieliikenneonnettomuudet_2015_onnettomuus.csv")
  val staticSchema = trafficAccidentDataFrame.schema
//  trafficAccidentDataFrame.printSchema()
  
  // Asks the user input in format "task n" where n is 1-6
  val name = scala.io.StdIn.readLine().split(" ")
  
  // Launches the right function matching the
  name(1).toInt match {
    case 1  => task1()
    case 2  => task2()
    case 3  => task3()
    case 4  => task4()
    case 5  => task5()
    case 6  => task6()
    // catch the default with a variable so you can print it
    case whoa  => println("Unexpected case: " + whoa.toString)
}

  println("Assignment Main Ending ------------------------------------------")
}