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
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import scala.math._

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

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import scala.collection.immutable.Vector

//import scala.reflect.internal.util.TableDef.Column

object Assignment extends App {
  println("Assignment Main Starting ----------------------------------------")
  
  def calculateVx(dayInt:Double): Double= {
	  return sin(2*Pi*dayInt/7)
	}
  val calculateVxUdf = udf(calculateVx _)
  
  def calculateVy(dayInt:Double): Double= {
	  return cos(2*Pi*dayInt/7)
	}
  val calculateVyUdf = udf(calculateVy _)
  
  def convertDayToInt(Vkpv:String): Int = {
    if (Vkpv == "Maanantai") {
    	return 1
		   
    } else if(Vkpv == "Tiistai") {
    	return 2
    			
    } else if(Vkpv == "Keskiviikko") {
    	return 3
    			
    } else if(Vkpv == "Torstai") {
    	return 4
    			
    } else if(Vkpv == "Perjantai") {
    	return 5
    			
    } else if(Vkpv == "Lauantai") {
    	return 6
    			
		} else if(Vkpv == "Sunnuntai") {
		  return 7
		   
		} else {
		   println("Day of the week not recognized")
		   return 999
		}
  }
  
  def task1() = {
  	// X and Y coordinate columns are taken from trafficAccidentDataFrame df
    val data: DataFrame = trafficAccidentDataFrame.select("X","Y").limit(1000)
    
    
    
		val vectorAssembler = new VectorAssembler()
		.setInputCols(Array("X", "Y"))
		.setOutputCol("features")

		import org.apache.spark.ml.Pipeline
		val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
		
		val pipeLine = transformationPipeline.fit(data) // Error
		val transformedTraining = pipeLine.transform(data)
		transformedTraining.show
		
		println("kohta 1")
		
		val kmeans = new KMeans()
		.setK(300)
		.setSeed(1L)
		
		println("kohta 2")
		
		val kmModel = kmeans.fit(transformedTraining)
		
		println("kohta 3")
		
		val centers = kmModel.clusterCenters
		
		scala.tools.nsc.io.File("results/basic.csv").appendAll("x,y" + "\n")
		for (center <- centers) {
			println(center(0) + "," + center(1))
			scala.tools.nsc.io.File("results/basic.csv").appendAll(center(0) + "," + center(1) + "\n")
		}
   }
  
  
  // Notes explaining the Task 2
  // We must divide the accidents on a unit circle, and calculate their x and y coordinates
  // with SIN and COS. After this the data is ready to be used in 3 dimensions.
  // Main problem in the adding of the weekday is that the weekdays monday and sunday for example
  // are considered to be far away from each other, and in reality we want the boundary to be cyclic
  // Meaning that also Monday and Sunday could be in the same cluster.
  // x=sin(2pi*dayOfTheWeekInt/7),y=cos(2pi*dayOfTheWeekInt/7)
  def task2() = {
      println("Task2 starting")
      println("Task Two Kohta 1 ----------------------------------------")
      
      // X and Y coordinate columns are taken from trafficAccidentDataFrame df
	    val data: DataFrame = trafficAccidentDataFrame.select("X","Y","Vkpv").limit(3000)
	    
			val turnWeekdayToInt = udf(convertDayToInt _)
	    
	    val dataWithDayInt = data.withColumn("dayInt", turnWeekdayToInt(col("Vkpv")))
	    
    	val dataWithVxCoordi = dataWithDayInt.withColumn("Vx", calculateVxUdf(col("dayInt")))
	    val dataWithVyCoordi = dataWithVxCoordi.withColumn("Vy", calculateVyUdf(col("dayInt")))
	    
	    val vectorAssembler = new VectorAssembler()
			.setInputCols(Array("X", "Y","Vx","Vy"))
			.setOutputCol("features")
			
			import org.apache.spark.ml.Pipeline
			val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
			
			val pipeLine = transformationPipeline.fit(dataWithVyCoordi) // Error
			val transformedTraining = pipeLine.transform(dataWithVyCoordi)
			
			val kmeans = new KMeans().setK(300).setSeed(1L)
			
			println("kohta 2")
			
			val kmModel = kmeans.fit(transformedTraining)
			
			println("kohta 3")
			
			var centers = kmModel.clusterCenters
			
			def turnRadToDays(rad:Double): Double= {
      	
      	if (rad < 0){
      		var tempRad = 6666.66
      		tempRad = Pi - rad
      		println((tempRad/(2*Pi))*7+1)
      		return (tempRad/(2*Pi))*7+1
      		
      	} else {
      		println((rad/(2*Pi))*7+1)
      		return (rad/(2*Pi))*7+1
      	}
			}
			scala.tools.nsc.io.File("results/task2.csv").appendAll("x,y,dow" + "\n")
			for (center <- centers) {
//				println(center(0) + "   " + center(1) + "   " + atan2(center(3), center(2)))
				
				scala.tools.nsc.io.File("results/task2.csv").appendAll(center(0) + "," + center(1) + "," +
								turnRadToDays(atan2(center(3), center(2))) + "\n")
			}
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
//  val name = scala.io.StdIn.readLine().split(" ")
  
  task2()
  // Launches the right function matching the
//  name(1).toInt match {
//    case 1  => task1()
//    case 2  => task2()
//    case 3  => task3()
//    case 4  => task4()
//    case 5  => task5()
//    case 6  => task6()
//    // catch the default with a variable so you can print it
//    case whoa  => println("Unexpected case: " + whoa.toString)
//}

  println("Assignment Main Ending ------------------------------------------")
}