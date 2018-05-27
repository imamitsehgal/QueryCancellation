package com.sapient.query


/**
  * Created by Amit on 18-05-2018.
  */
object QueryAPI {

  //TODO get a callback for the results too
  def runQuery(userId:Int,query:String):Long={
    QueryServer.runQuery(userId,query)
  }

  def killQuery(user: Int,queryId:Long)={
    QueryServer.killQuery(user,queryId)
  }

  def checkStatus(queryId:Long)={

  }

}
