package org.mj.flink.http.akka

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import org.mj.flink.akka.retry.BackOffRetry

import scala.collection.GenSeq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object AkkaHttpClient extends LazyLogging {
  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  private implicit val as: ActorSystem = ActorSystem("mj_http")
  private implicit val mat: ActorMaterializer = ActorMaterializer()

  private lazy val httpClient = new AkkaHttpClient
  private lazy val retryHandler = new BackOffRetry


  def main(args: Array[String]): Unit = {
    val url = "http://example.org/"
    val getRequest = HttpRequest(uri = url)
    val postRequest = HttpRequest(method = HttpMethods.POST, uri = url, entity = "{json:1}")

    val times = 34
    val httpRequests = Array.fill(times / 2)(getRequest) ++ Array.fill(times / 2)(postRequest)
    //Random.shuffle(requests)

    val httpResponses = httpClient.runHttpRequests(httpRequests)
    //    Thread.sleep(5000)
    //    httpResponses.flatMap(_).foreach(r => {})
    //    val r = httpResponses.map(_.value).toSeq
    retryHandler.printResult(httpResponses)
    //    httpResponses.map(retryHandler.printResult(_))

    logger.info(s"Final Results ${httpResponses.value}")
    println(s"Final results ${httpResponses.value.get}")
    //    httpResponses.map(_.value.get).map(_.isSuccess).groupBy(identity).mapValues(_.size).map(println)

    System.exit(-1)
  }
}

sealed case class CIHttpRespose(responseCode: Int, responseBody: String) {}

class AkkaHttpClient extends LazyLogging {
  private lazy val retryHandler = new BackOffRetry

  def runHttpRequests(httpRequests: GenSeq[HttpRequest])(implicit ac: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer) = { //: Future[GenSeq[HttpResponse]] = {
    val httpResponses = for (httpRequest <- httpRequests) yield {
      val responseFuture = retryHandler.retryWithBackOff(Http(ac).singleRequest(httpRequest), initialWaitInMS = 10, maxAllowedWaitInMS = 60000, maxAllowedRetryCount = 20)
      responseFuture.onComplete {
        case Success(httpResponse) =>
          logger.info(s"Finished[${httpRequest.method} ${httpRequest.uri}] : ${httpResponse.status}")
        case Failure(ex) =>
          logger.error(s"Failed: $ex")
      }

//      val responseBody = for {
//        response <- responseFuture
//        entity <- Unmarshal(response.entity).to[ByteString]
//      } yield entity.utf8String
      val responseBody = responseFuture.flatMap(r => Unmarshal(r.entity).to[ByteString]).map(_.utf8String)
      //val responseCode = responseFuture.map(_.status)

      //responseFuture
      responseBody
    }
    Future.sequence(httpResponses.toList)
  }

  //  def runHttpRequest(httpRequest: HttpRequest)(implicit ac: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer): Future[HttpResponse] = {
  //    val res = runHttpRequests(Seq(httpRequest))
  //    res.map(_.head)
  //  }
  def runHttpRequest(httpRequest: HttpRequest)(implicit ac: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer): Future[Array[String]] = {
    val responseFuture = for {
      response <- Http(ac).singleRequest(httpRequest)
      entity <- {
        val result = Unmarshal(response.entity).to[ByteString]
        result.map(r => r.utf8String.split(" "))
      }
    } yield entity

    responseFuture.onComplete {
      case Success(entity) => {
        logger.info(s"Finished[${httpRequest.uri}] : ${entity.length}")
      }
      case Failure(ex) => {
        logger.error(s"Failed: $ex")
      }
    }
    responseFuture
  }
}
