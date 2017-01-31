package org.mj.flink

import java.util.concurrent.Executors
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import akka.actor.ActorSystem
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }
import akka.actor.ActorSystem
import akka.pattern.after
import akka.pattern.after
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.nio.client.HttpAsyncClients
import org.mj.file.FileHelper
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch

object Futures {
  // TODO: Add timeouts
  private val LOG = LoggerFactory.getLogger(getClass)

  val maxThreads = 1000
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(maxThreads))

  val iterations = 100000
  var finishCnt: AtomicInteger = new AtomicInteger(0)
  var failedCnt: AtomicInteger = new AtomicInteger(0)
  val doneSignal = new CountDownLatch(iterations)

  def main(args: Array[String]): Unit = {
    println("START")
    testAkkaFutures
    //testScalaFutures
    println("END")
    var ack = 0
    while(iterations > ack) {
      ack = finishCnt.get + failedCnt.get
      println(s"latch ${doneSignal.getCount}: ${iterations-(finishCnt.get + failedCnt.get)} = $iterations - (${finishCnt.get} + ${failedCnt.get})")
      Thread.sleep(1000)
    }
  }

  def testAkkaFutures = {
    val system = ActorSystem("theSystem")
    for (itr <- 1 to iterations) {
      val future = getRandomFuture(s"f-$itr")
      lazy val t = after(duration = 1 second, using = system.scheduler)(Future.failed(new TimeoutException("Future timed out!")))

      future.onComplete {
        case Success(result) => {
          doneSignal.countDown
          finishCnt.incrementAndGet
          //LOG.info(s"Success: ${result._1} :: ${result._2}")
        }
        case Failure(ex) => {
          doneSignal.countDown
          failedCnt.incrementAndGet
          //LOG.error(s"Failure :: ${ex}")
        }
      }
    }
    system.shutdown
  }

    def testScalaFutures = {
    for (itr <- 1 to iterations) {
      val future = getRandomFuture(s"f-$itr")
      //      Await.result(future, 5.second)
      future.onComplete {
        case Success(result) => {
          finishCnt.incrementAndGet // += 1
          //LOG.info(s"Success: $fName :: ${result}")
        }
        case Failure(ex) => {
          failedCnt.incrementAndGet // += 1
          //LOG.error(s"Failure: $fName :: ${ex}")
        }
      }
    }
  }

  def getRandomFuture(name: String): Future[(String, String)] = Future {
    val result = Random.nextInt(10) / Random.nextInt(10)
    val sleepTime = 100 + Random.nextInt(10)
    Thread.sleep(sleepTime)
    (name, s"future $name slept $sleepTime ms, result = $result")
  }
}