import org.apache.spark.sql.{SQLContext, SparkSession}
import java.io.IOException;

object projectMain {
@throws(classOf[IOException])
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkHealthCareService")
      .getOrCreate()

    val healthData = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("E:/SparkFiles")



      healthData.createOrReplaceTempView("health_Data")

  /**
    * This code is working
    */
//  println("+++++++++++++++++++++++++++++++ Health Data Showing in table form +++++++++++++++++++++++++++++++")
//  healthData.show(false)


  /**
    * This code is working
    * Below are codes for spark sql
    */
//  println("+++++++++++++++++++++++++++++++ Health Data Showing in table form Using SparkSQL +++++++++++++++++++++++++++++++")

//  val sqlWay = spark.sql("select * from health_data")
//  sqlWay.explain()

//  spark.sql("select `Provider Name`, count(1) from health_data group by `Provider Name`").show(false)


  /**
    * This code is working
    * Below are codes for working with dataframes
    */
//  val dataFrameWay = healthData
//      .groupBy("Provider Name")
//      .count()
//
//  dataFrameWay.show(false)
  /**
    * This code is working.
    */
//  dataFrameWay.explain()
//  println("\n" + "********************Showing Schema***************************")
//  healthData.printSchema()
    }//end of main


}//end of object
