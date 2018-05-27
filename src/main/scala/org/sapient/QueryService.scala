package org.sapient

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorSystem
import akka.actor.Status.{Failure, Success}
import akka.http.javadsl.server.Route
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.{HttpResponse, StatusCode, StatusCodes}

import scala.collection.JavaConverters._
import spray.json.{DefaultJsonProtocol, JsArray, pimpAny}
import spray.json.DefaultJsonProtocol._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait QueryService extends SparkService {

  implicit val system: ActorSystem
  implicit val materializer : ActorMaterializer
 // implicit val sparkSession: SparkSession
 // val datasetMap = new ConcurrentHashMap[String, Dataset[Row]]()
  implicit val blockingDispatcher = system.dispatchers.lookup("my-blocking-dispatcher")


  val route =
    pathSingleSlash {
      get {
        complete {
          "welcome to rest service"
        }
      }
    } ~
      path("runQuery" / "county"/Segment) { county  =>
        get {
          complete{
            var res= ""
            val documentId = "user ::" + UUID.randomUUID().toString
            val queryId = System.nanoTime().toString
            val stmt = "select a.sex,count(*) from data_tbl a,data_tbl b where b.county=a.county group by a.sex"
            val result = runQuery(stmt,queryId)
           /* var entity = queryResult match {
              case Some(result) =>s"Query : $stmt  is submitted. Query id is $result. User id is $documentId"
              case None => s"Query : $stmt could not be submitted. User id is $documentId"
            }*/

            var entity = s"Query : $stmt  is submitted. Query id is $queryId. User id is $documentId"
            entity
          }
        }
      } ~
      path("getStatus" / """[\w[0-9]-_]+""".r) { id =>
        get {
          complete {

            val statusResult = getStatus(id)
            var res = statusResult match {
              case Some(result) =>  s"Status for query id : $id is $result"
              case None =>  s"Could not find the status of the query id : $id"
            }

            res
          }
        }
      } ~
      path("killQuery" / """[\w[0-9]-_]+""".r) { id =>
        get {
          complete {

            val statusResult = killQuery(id)
            s"Query id $id is cancelled."
          }
        }
      }
}
