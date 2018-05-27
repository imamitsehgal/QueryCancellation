package com.sapient.query

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup

/**
  * Created by Amit on 18-05-2018.
  */
object QueryServer {

  val threadPoolName = "QueryProcessorPool"
  val backgroundOperationPool = new ThreadPoolExecutor(1, 10 , 10, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](1000), new ThreadFactoryWithGarbageCleanup(threadPoolName))
  backgroundOperationPool.allowCoreThreadTimeOut(true)

  def runQuery(user:Int,query:String):Long={
    val queryId = System.nanoTime()
    println(s"Query submitted $query with queryID=$queryId")
    backgroundOperationPool.submit(SparkRunner.runQueryTask(user,query,queryId))
    queryId
  }



  def killQuery(userId:Int,queryId:Long)=
  SparkRunner.killJobsForQuery(queryId,userId)





}
