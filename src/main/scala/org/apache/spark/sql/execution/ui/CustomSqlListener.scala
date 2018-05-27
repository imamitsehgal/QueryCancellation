package org.apache.spark.sql.execution.ui

import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.streaming.StreamingQueryStatus

class CustomSqlListener (ss : SparkConf, queryJobMapStart : Map[String, String], queryStatusMap: Map[String,String]) extends SQLListener(ss) {


  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
   //println("Job Submitted with details :- JobID : " + jobStart.jobId)
   // println(" Job Query Id : " + jobStart.properties.getProperty("callSite.short"))
    val jobId = jobStart.jobId
    val queryId: String = jobStart.properties.getProperty("callSite.short").split(", ")(1).split("=")(1)
    //println("*********"+queryId + "->" + jobId)
    queryJobMap addBinding (queryId.toLong,jobId)
    //println(queryJobMap)
    queryJobMapStart + (queryId -> jobId)
    queryJobMapStart.foreach(x => print(x._1, x._2))
    jobStatusMap = jobStatusMap + (jobStart.jobId -> "Running")
    super.onJobStart(jobStart)

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
 //   println(s"Job id ${jobEnd.jobId} is completed")
    jobStatusMap = jobStatusMap + (jobEnd.jobId -> "Complete")
    queryStatusMap + (jobEnd.jobId.toString -> "Completed")
    println("--------------------------------------------------------")
    queryStatusMap.foreach(x => print(x._1, x._2))
    jobEnd.jobResult
    super.onJobEnd(jobEnd)
  }

  import collection.mutable
  //TODO clean this map regularly.
  val queryJobMap = new mutable.HashMap[Long, mutable.Set[Int]] with mutable.MultiMap[Long, Int]
  var jobStatusMap = Map[Int,String]()

  def getRunnigJobsForQuery(queryId:Long):Set[Int]={
    val jobids= queryJobMap.get(queryId).getOrElse(Nil)
    jobids.filter(x=>(jobStatusMap.getOrElse(x,"").equals("Running"))).toSet
  }
}