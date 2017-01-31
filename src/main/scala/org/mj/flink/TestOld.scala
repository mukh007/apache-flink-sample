//package org.mj.flink
//
//import scala.concurrent.Future
//import scala.concurrent.duration._
//
//import akka.actor.ActorSystem
//import akka.util.Timeout
//import akka.pattern.ask
//import akka.io.IO
//
//import spray.can.Http
//import spray.http._
//import HttpMethods._
//import spray.http.HttpHeaders.Authorization
//import org.joda.time.DateTime
//import org.joda.time.Seconds
//
//object Test {
//  def main(args: Array[String]): Unit = {
//    println("Hello MJ!")
//    implicit val system: ActorSystem = ActorSystem()
//    implicit val timeout: Timeout = Timeout(150.seconds)
//    import system.dispatcher // implicit execution context
//    val iterations = 1000
//    var requestCount = 0
//    var responseCount = 0
//
//    var start = DateTime.now()
//    val semanticApiAuth = BasicHttpCredentials("bc35d8f22c1eea58", "ff0d1cc42d8f672923689ef5")
//    val httpFutures = new java.util.ArrayList[Future[HttpResponse]]()
//
//    (1 to iterations) foreach { itr => {
//        val semanticApiRequest = HttpRequest(
//          GET,
//          Uri("http://slc06caf.oracle.com/semanticApi/v1/metadata-catalog"),
//          List(Authorization(semanticApiAuth)))
//        val semanticApiResponse: Future[HttpResponse] = (IO(Http) ? semanticApiRequest).mapTo[HttpResponse]
//        semanticApiResponse.onComplete {
//          println("semanticApiResponse " + itr)
//          x => print(x.get.status + " ")
//          responseCount += 1
//        }}
//      requestCount += 1
//    }
//
//    import spray.httpx.RequestBuilding._
//    val response2: Future[HttpResponse] = (IO(Http) ? Get("http://wttr.in")).mapTo[HttpResponse]
//
//    response2.onComplete {
//      print("response2")
//      x => println(x.get.status)
//    }
//
//    while (responseCount != requestCount) {
//      println(s"Finished ($responseCount/$requestCount)")
//      Thread.sleep(1000)
//    }
//    var end = DateTime.now()
//    println("\nFINISHED\n")
//    println("GoodBye MJ!, took time " +  (end.getMillis-start.getMillis) + " ms")
//    system.shutdown()
//  }
//}