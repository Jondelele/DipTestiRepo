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
  trafficAccidentDataFrame.printSchema()
  
  // Asks the user input in format "task n" where n is 1-6
  val name = scala.io.StdIn.readLine().split(" ")
  
  // Launches the right function matching the
  name(1).toInt match {
  	case 1  => println("task 1 selected")
	  case 2  => println("task 2 selected")
	  case 3  => println("task 3 selected")
	  case 4  => println("task 4 selected")
    case 5  => println("task 5 selected")
    case 6  => println("task 6 selected")
    // catch the default with a variable so you can print it
    case whoa  => println("Unexpected case: " + whoa.toString)
}

	println("Assignment Main Ending ------------------------------------------")
}