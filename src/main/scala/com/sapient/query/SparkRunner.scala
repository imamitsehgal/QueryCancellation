package com.sapient.query

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.ui.CustomSqlListener
import org.apache.spark.sql.util.QueryExecutionListener

/**
  * Created by Amit on 18-05-2018.
  */
object SparkRunner {


  //spark-submit --conf spark.scheduler.mode"FAIR"


  val session = SparkSession
    .builder()
    //TODO make this
    .config("spark.scheduler.mode", "FAIR")
    .config("hive.exec.local.scratchdir","/tmp/local/scratch")
    .appName("QueryCancellation")
    .master("local[3]")
    .enableHiveSupport()
    .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)

  var queryJobMapStart = Map[String, String]()
  var queryStatusMap = Map[String,String]()

  session.sparkContext.setLogLevel("WARN")

  val dataDF = session.read
    .format("csv")
    .option("inferSchema","true")
    .option("header","true")
    .load("F:\\dev\\QueryCancellation\\src\\main\\resources\\Baby_Names__Beginning_2007.csv")




  session.listenerManager.register(new QueryExecutionListener {
    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {println("Failed")}

    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
      println(funcName+" "+qe.executedPlan+" "+qe.toStringWithStats)
    }
  })
  dataDF.createOrReplaceTempView("data_tbl")

  val customListener = new CustomSqlListener(session.sparkContext.getConf,queryJobMapStart,queryStatusMap)
  val appListener = session.sparkContext.addSparkListener(customListener)

  def runQueryTask(userId:Int,query:String,queryID:Long):Runnable={
    new QueryOperation(userId.toString,queryID,query,session)
  }

  //TODO do this asynch too

  def killJobsForQuery(queryId:Long,userId:Int): Unit ={

    val runningJobsToKill = customListener.getRunnigJobsForQuery(queryId)
    println("--------------------------------------------------------")
    println(s"Going to kill JobID=$runningJobsToKill for query $queryId")
    println("--------------------------------------------------------")
    runningJobsToKill.foreach(session.sparkContext.cancelJob(_,"Killed By User="+userId))

    println("--------------------------------------------------------")
    println(s"Going to kill JobID=$runningJobsToKill for query $queryId")
    println("--------------------------------------------------------")
  }

}
