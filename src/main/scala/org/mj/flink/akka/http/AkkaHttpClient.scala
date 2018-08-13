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
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

object AkkaHttpClient extends LazyLogging {
  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  private implicit val as: ActorSystem = ActorSystem("mj_http")
  private implicit val mat: ActorMaterializer = ActorMaterializer()

  private lazy val httpClient = new AkkaHttpClient

  def main(args: Array[String]): Unit = {
    val url = "http://example.org/"
    val getRequest = HttpRequest(uri = url)
    val postRequest = HttpRequest(method = HttpMethods.POST, uri = url, entity = "{json:1}")

    val times = 34
    val httpRequests = Array.fill(times / 2)(getRequest) ++ Array.fill(times / 2)(postRequest)

    val httpResponses = httpClient.runHttpRequests(httpRequests)
    Await.result(httpResponses, Duration.Inf)

    logger.info(s"Final Results ${httpResponses.value}")
    println(s"Final results ${httpResponses.value.get}")
    //    httpResponses.map(_.value.get).map(_.isSuccess).groupBy(identity).mapValues(_.size).map(println)

    System.exit(-1)
  }
}

sealed case class MyHttpRespose(responseBody: String, responseCode: StatusCode)

class AkkaHttpClient extends LazyLogging {
  private lazy val retryHandler = new BackOffRetry

  def runHttpRequests(httpRequests: GenSeq[HttpRequest])(implicit ac: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer): Future[GenSeq[MyHttpRespose]] = {
    val httpResponses = for (httpRequest <- httpRequests) yield {
      val httpFutureRequest = {
        if (Random.nextInt(100) < 0) // Inject some % failures
          Future.failed(new IllegalArgumentException("random failure"))
        else
          Http(ac).singleRequest(httpRequest)
      }
      val responseFuture = retryHandler.retryWithBackOff(httpFutureRequest, backOffFactor = 1.5f, initialWaitInMS = 1, maxAllowedWaitInMS = 4000, maxAllowedRetryCount = 20)
      responseFuture.onComplete {
        case Success(httpResponse) =>
          //Thread.sleep(100 + Random.nextInt(100)) // Mock service call delay
          logger.debug(s"Finished[${httpRequest.method} ${httpRequest.uri}] : ${httpResponse.status}")
        case Failure(ex) =>
          logger.error(s"Failed: $ex")
      }

      val responseBody = for {
        response <- responseFuture
        entity <- Unmarshal(response.entity).to[ByteString]
      } yield MyHttpRespose(entity.utf8String, response.status)
      responseBody
    }
    Future.sequence(httpResponses.toVector)
  }

  def runHttpRequests(httpRequest: HttpRequest)(implicit ac: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer): Future[MyHttpRespose] = {
    runHttpRequests(Seq(httpRequest)).map(_.head)
  }
}
