package org.mj.flink

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

object TestApp extends App with LazyLogging {
  private implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1000))

  private def getFuture(in: Int) = {
    val rand = Random.nextInt(1000)
    val f = Future {
      Thread.sleep(rand)
      rand
    }
    f.onComplete(_ => logger.info(s"Finished $in -> $rand"))
    f
  }

  val futuresList = for (i <- 1 to 100) yield getFuture(i)
  val result = Future.traverse(futuresList)(identity)//(x => x.zip ?)
  val result1 = Future.sequence(futuresList)

  result.onComplete {
    case Success(x) => logger.info(s"result = $x")
    case Failure(e) => e.printStackTrace()
  }

  logger.info("Futures scheduled")
  Await.result(result, Duration.Inf)

  logger.info(s"Finished futures ${result.value}")
}
