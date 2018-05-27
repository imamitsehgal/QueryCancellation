package org.sapient

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer

import scala.concurrent.Future
//import scala.concurrent.ExecutionContext.Implicits.global

class QueryServer (implicit val system:ActorSystem ,
                   implicit val materializer: ActorMaterializer) extends QueryService {

  def startServer(address : String, port: Int) = {
      Http().bindAndHandle(route,address,port)
  }

}

object QueryServer extends App {

  implicit val actorSystem = ActorSystem("query-server")
  implicit val materializer = ActorMaterializer()
  val server = new QueryServer()
  server.startServer("localhost",8080)
  println("running server at localhost 8080")

}
