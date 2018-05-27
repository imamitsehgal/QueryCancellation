package org.sapient

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.execution.ui.CustomSqlListener
import org.apache.spark.sql.{Row, SparkSession}

import scala.concurrent.{ExecutionContext, Future}


trait SparkService {

  val session = SparkSession
                .builder()
                .config("spark.scheduler.mode", "FAIR")
                .appName("QueryCancellation")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate()

  var queryJobMapStart = Map[String, String]()
  var queryStatusMap = Map[String,String]()

  session.sparkContext.setLogLevel("ERROR")

  session.sparkContext.setCallSite("Reading the file")

  val dataDF = session.read
      .format("csv")
      .option("inferSchema","true")
      .option("header","true")
      .load("C:\\dev\\QueryCancellation\\src\\main\\resources\\Baby_Names__Beginning_2007.csv")



  dataDF.createOrReplaceTempView("data_tbl")


  //dataDF.printSchema()

  val customListener = new CustomSqlListener(session.sparkContext.getConf,queryJobMapStart,queryStatusMap)
  val appListener = session.sparkContext.addSparkListener(customListener)

  def runQuery(query : String, queryId: String)=  {


  //  println("queryId: "+ queryId +" query:" + query)
    session.sparkContext.setLocalProperty("callSite.short",queryId + " query:" + query)
   // session.sparkContext.setLocalProperty("callSite.long",query)

    val data = session.sql(query)
    data.show()
    //Thread.sleep(60000)
   // Future(data)
  }


  def getStatus(queryID: String) : Option[String] = {

    val jobId: Option[String] = queryStatusMap.get(queryID)
    println(s"----------------Job id is $jobId")

    val status = jobId match {
      case Some(value) => queryStatusMap.get(value)
      case None => Option("Running")
    }
    status
  }

  def killQuery(queryID: String) = {

    val jobId: Option[String] = queryJobMapStart.get(queryID)
    print(s"-----------------kill query job id $jobId")

    val cancellingStatus = jobId match {
      case Some(value) => session.sparkContext.cancelJob(value.toInt, "user requested")
      case None => None
    }


  }



}

object SparkService extends SparkService
