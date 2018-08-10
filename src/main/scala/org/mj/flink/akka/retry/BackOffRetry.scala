package org.mj.flink.akka.retry

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.pattern.Patterns.after
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.mj.flink.http.akka.AkkaHttpClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

object BackOffRetryException extends Throwable

object BackOffRetry extends App with LazyLogging {
  private implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global
  private implicit val as: ActorSystem = ActorSystem("mj_retry")
  private implicit val mat: ActorMaterializer = ActorMaterializer()

  private lazy val retryHandler = new BackOffRetry
  val retryFuture = retryHandler.retryWithBackOff(getFutureRequest(Random.nextInt(50)), factor = 10f, initialWaitInMS = 1)
  //retryHandler.printResult(retryFuture)

  private lazy val httpHandler = new AkkaHttpClient
  val url = "http://example.org/"
  val postRequest = HttpRequest(method = HttpMethods.POST, uri = url, entity = "{json:1}")
  //  val httpFuture = retryHandler.retryWithBackOff(httpHandler.runHttpRequest(postRequest))
  //  printResult(httpFuture)

  val httpResponses = httpHandler.runHttpRequests(Seq.fill(100)(postRequest))
  //  retryHandler.printResult(httpResponses)
  //  httpResponses.map(retryHandler.printResult(_))
  Await.result(httpResponses, Duration.Inf)
  println(s"\n\nFinal results :: ${httpResponses.value.get.get.size}")
  //  httpResponses.map(_.value.get).map(_.isSuccess).groupBy(identity).mapValues(_.size).map(println)
  httpResponses.value.map(_.isSuccess).groupBy(identity).mapValues(_.size).map(println)
  //  httpResponses.value.flatMap(_.get).map(_.status.intValue).groupBy(identity).mapValues(_.size).map(println)

  System.exit(-1)

  private def getFutureRequest(id: Int): Future[String] = {
    val rand = Random.nextInt(100)
    val f = if (rand < 80) // will fail 80% times
      Future.failed(BackOffRetryException)
    else
      Future.successful(s"<data-$id@${System.currentTimeMillis}>")
    f
  }

  sys.addShutdownHook({
    logger.info(s"Terminating $as")
    as.terminate()
  })
}

class BackOffRetry extends LazyLogging {
  private val RETRY_MAX_COUNT = 5
  private val RETRY_INIT_WAIT_MS = 5
  private val RETRY_MAX_WAIT_TIME_MS = 100
  private val RETRY_BACK_OFF_FACTOR = 1.5f

  def retryWithBackOff[T](futureRequest: => Future[T], factor: Float = RETRY_BACK_OFF_FACTOR, initialWaitInMS: Int = RETRY_INIT_WAIT_MS, currentWaitInMS: Int = 0, totalWaitInMS: Int = 0, maxAllowedWaitInMS: Int = RETRY_MAX_WAIT_TIME_MS, totalRetryCount: Int = 0, maxAllowedRetryCount: Int = RETRY_MAX_COUNT)(implicit as: ActorSystem, ec: ExecutionContext): Future[T] = {
    def attemptRetry(e: Exception): Future[T] = {
      val nextWaitInMS: Int = if (currentWaitInMS == 0) initialWaitInMS else Math.ceil(currentWaitInMS * factor).toInt
      logger.warn(s"Retrying #($totalRetryCount/$maxAllowedRetryCount), after $nextWaitInMS ms ($totalWaitInMS/$maxAllowedWaitInMS) ${e.getClass.getSimpleName}")
      if (maxAllowedWaitInMS <= totalWaitInMS | maxAllowedRetryCount <= totalRetryCount)
        Future.failed(new TimeoutException(s"Failing as max retries($totalRetryCount/$maxAllowedRetryCount) or time($totalWaitInMS/$maxAllowedWaitInMS ms) exceeded"))
      else after(nextWaitInMS.milliseconds, as.scheduler, ec, Future.successful(1)).flatMap { _ =>
        retryWithBackOff(futureRequest, factor, initialWaitInMS, nextWaitInMS, totalWaitInMS + nextWaitInMS, maxAllowedWaitInMS, totalRetryCount + 1, maxAllowedRetryCount)
      }
    }

    futureRequest.recoverWith {
      case e: Exception => attemptRetry(e)
      case t: Throwable => throw t
    }
  }
}
