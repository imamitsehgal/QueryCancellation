package com.sapient.query

/**
  * Created by Amit on 18-05-2018.
  */
object QueryClient extends App{

  val query = "select a.sex,count(*) from data_tbl a,data_tbl b where b.county=a.county group by a.sex"

  var queryToKill:Long = -1l


  for(i <- 1 to 2){
    println(s"User$i submitting query")
    val queryId = QueryAPI.runQuery(i,query)
    if(queryToKill == -1 )queryToKill=queryId
    println(s"User$i got queryId=$queryId")
  }

  Thread.sleep(20000)
  println("\n----------------------After some time----------------------------------")
  println(s"!!!!Got a kill request for queryId=$queryToKill by user="+1+"!!!!")
  println("--------------------------------------------------------")



  QueryAPI.killQuery(1,queryToKill)

}
