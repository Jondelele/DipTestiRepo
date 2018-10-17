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
  
  // Turns radians into weekdays (1-7)
  def turnRadToDays(rad:Double): Double= {
  	if (rad < 0){
  		var tempRad = 0.0
  		tempRad = Pi - rad
  		return (tempRad/(2*Pi))*7+1
  		
  	} else {
  		return (rad/(2*Pi))*7+1
  	}
	}
  
  // Calculates the weekdays X coordinate and returns it
  def calculateWx(dayInt:Double): Double= {
	  return sin(2*Pi*dayInt/7)
	}
  val calculateWxUdf = udf(calculateWx _)
  
  // Calculates the weekdays Y coordinate and returns it
  def calculateWy(dayInt:Double): Double= {
	  return cos(2*Pi*dayInt/7)
	}
  val calculateWyUdf = udf(calculateWy _)
  
  // Converts the Vkpv from string into int, so it can be 
  // used in task 2 calculations
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
  val turnWeekdayToInt = udf(convertDayToInt _)
  
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
  
  // Function which holds the iplementation of task 2
  def task2() = {
      // X, Y and Vkpv columns are taken from trafficAccidentDataFrame, because
      // everything else is unnecessary DataFrame size is limited to 3000 because bigger
  		// samples cause out of memory issue
	    val data: DataFrame = trafficAccidentDataFrame.select("X","Y","Vkpv").limit(3000)
	    
//			val turnWeekdayToInt = udf(convertDayToInt _)
			
			// Adds new column to the DataFrame which holds the days as int (1-7)
	    val dataWithDayInt = data.withColumn("dayInt", turnWeekdayToInt(col("Vkpv")))
	    
	    // Adds the X and Y coordinates of the days in to the DataFrame functions
	    // calculateWxUdf and calculateWyUdf are defined on top of the file
    	val dataWithWxCoordi = dataWithDayInt.withColumn("Wx", calculateWxUdf(col("dayInt")))
	    val dataWithWxWyCoordi = dataWithWxCoordi.withColumn("Wy", calculateWyUdf(col("dayInt")))
	    
	    // K-means requires the data in vector form, so it is changed to vector
	    val vectorAssembler = new VectorAssembler()
			.setInputCols(Array("X", "Y","Wx","Wy"))
			.setOutputCol("features")
			
			import org.apache.spark.ml.Pipeline
			val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
			
			val pipeLine = transformationPipeline.fit(dataWithWxWyCoordi)
			val transformedTraining = pipeLine.transform(dataWithWxWyCoordi)
			
			// Calculates the kmeans for 300 clusters and random seed
			val kmeans = new KMeans().setK(300).setSeed(1L)
			
			val kmModel = kmeans.fit(transformedTraining)
			var centers = kmModel.clusterCenters
      
			// Adds the line "x,y,dow" in the top of the result file
			scala.tools.nsc.io.File("results/task2.csv").appendAll("x,y,dow" + "\n")
			
			// This for-loop turns weekday coordinates into days (1-7) and appends
			// the lines into result csv file
			for (center <- centers) {
				scala.tools.nsc.io.File("results/task2.csv").appendAll(center(0) + "," + center(1) + "," +
								turnRadToDays(atan2(center(3), center(2))) + "\n")
			}
		}
  
  def task4() = {
    
    println("Task4 alkaa")
      
    // X and Y coordinate columns are taken from trafficAccidentDataFrame df
    val data: DataFrame = trafficAccidentDataFrame.select("X","Y").limit(1000)
    
		val vectorAssembler = new VectorAssembler()
		.setInputCols(Array("X", "Y"))
		.setOutputCol("features")

		import org.apache.spark.ml.Pipeline
		val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
		
		val pipeLine = transformationPipeline.fit(data)
		val transformedTraining = pipeLine.transform(data)
		
//		transformedTraining.show
		
		println("kohta 1")
		
		import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
    import org.apache.spark.ml.linalg.Vectors
    
    var k = 0
    
    // For-loop calculates the Within Set Sum of Squared Errors for the
    // amount of clusters of our choosing
    for ( k <-50 to 100){		
       val kmeans = new KMeans()
  		.setK(k)
  		.setSeed(1L)
  		val model = kmeans.fit(transformedTraining)
  		// Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = model.computeCost(transformedTraining)
      //println(i + ":  " + s"Within Set Sum of Squared Errors = $WSSSE")
      println(WSSSE)
   	}

		println("Here starts the Task 4 Third Dimension elbow task ---------------------------------------------------------------") 
    val dataVkpv: DataFrame = trafficAccidentDataFrame.select("X","Y","Vkpv").limit(3000)
		
		// Adds new column to the DataFrame which holds the days as int (1-7)
    val dataWithDayInt = dataVkpv.withColumn("dayInt", turnWeekdayToInt(col("Vkpv")))
    
    // Adds the X and Y coordinates of the days in to the DataFrame functions
    // calculateWxUdf and calculateWyUdf are defined on top of the file
  	val dataWithWxCoordi = dataWithDayInt.withColumn("Wx", calculateWxUdf(col("dayInt")))
    val dataWithWxWyCoordi = dataWithWxCoordi.withColumn("Wy", calculateWyUdf(col("dayInt")))
    
    // K-means requires the data in vector form, so it is changed to vector
    val vectorAssemblerThird = new VectorAssembler()
		.setInputCols(Array("X", "Y","Wx","Wy"))
		.setOutputCol("features")
		
		import org.apache.spark.ml.Pipeline
		val transformationPipelineThird = new Pipeline().setStages(Array(vectorAssemblerThird))
		
		val pipeLineThird = transformationPipelineThird.fit(dataWithWxWyCoordi)
		val transformedTrainingThird = pipeLineThird.transform(dataWithWxWyCoordi)
		
		k = 0
    // For-loop calculates the Within Set Sum of Squared Errors for the
    // amount of clusters of our choosing
    for ( k <-50 to 100){		
       val kmeans = new KMeans()
  		.setK(k)
  		.setSeed(1L)
  		val model = kmeans.fit(transformedTrainingThird)
  		// Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSEthird = model.computeCost(transformedTrainingThird)
      //println(i + ":  " + s"Within Set Sum of Squared Errors = $WSSSEthird")
      println(WSSSEthird)
   	}
		
		println("Task 4 loppuu")
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
  
  // Launches the right function matching the
//  name(1).toInt match {
  args(1).toInt match {
    case 1  => task1()
    case 2  => task2()
    case 4  => task4()
    case 5  => task5()
    case 6  => task6()
    // catch the default with a variable so you can print it
    case whoa  => println("Unexpected case: " + whoa.toString)
}

  println("Assignment Main Ending ------------------------------------------")
}