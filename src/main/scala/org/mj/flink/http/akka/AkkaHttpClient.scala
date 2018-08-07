package org.mj.flink.http.akka

import java.util.concurrent.CountDownLatch

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success}

object AkkaHttpClient extends LazyLogging {
  lazy val httpClient = new AkkaHttpClient

  def main(args: Array[String]): Unit = {
    val url = "http://example.org/"
    val getRequest = HttpRequest(uri = url)
    val postRequest = HttpRequest(method = HttpMethods.POST, uri = url, entity = "{json:1}")

    val times = 100
    val requests = Array.fill(times / 2)(getRequest) ++ Array.fill(times / 2)(postRequest)
    //Random.shuffle(requests)

    val latch = new CountDownLatch(requests.length)
    val start = System.currentTimeMillis()
    val results = for (request <- requests) yield {
      val resF = httpClient.doHttpRequest(request, Some(latch))
    }
    latch.await()
    val end = System.currentTimeMillis()
    logger.info(s"All ${requests.size} requests done in ${end - start}ms")
    System.exit(-1)
  }
}

class AkkaHttpClient extends LazyLogging {
  private implicit lazy val system = ActorSystem("mj")
  private implicit lazy val materializer = ActorMaterializer()
  private implicit lazy val executionContext = system.dispatcher
  private implicit lazy val http = Http(system)

  sys.addShutdownHook({
    system.terminate()
    materializer.shutdown()
  })

  def doHttpRequest(httpRequest: HttpRequest, latch: Option[CountDownLatch] = None) = {

    val responseFuture = for {
      response <- http.singleRequest(httpRequest)
      entity <- Unmarshal(response.entity).to[ByteString]
    } yield (response, entity)

    responseFuture.onComplete {
      case Success((response, entity)) => {
        logger.info(s"Finished[${response.status} ${httpRequest.method}] : ${entity.utf8String.length}")
        if (latch.isDefined) latch.get.countDown()
      }
      case Failure(ex) => {
        logger.error(s"Failed: $ex")
        if (latch.isDefined) latch.get.countDown()
      }
    }
    responseFuture
  }


  /**
    * External
    */
  def runHttpRequest(httpRequest: HttpRequest) = {
    val responseFuture = for {
      response <- http.singleRequest(httpRequest)
      entity <- {
        val result = Unmarshal(response.entity).to[ByteString]
        result.map(_.utf8String.split(" "))
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
