package com.sapient.query

import org.apache.spark.sql.SparkSession

/**
  * Created by Amit on 18-05-2018.
  */
class QueryOperation(user:String,queryId:Long,query:String,session:SparkSession) extends Runnable{
  override def run(): Unit = {
    Thread.sleep(100)

    session.sparkContext.setCallSite(s"User=$user, QueryId=$queryId, Query=$query")
    session.time(session.sql(query).count())
    println(s"QueryID=${queryId} completed")
  }
}
