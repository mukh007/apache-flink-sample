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

object Test {
  private val LOG = LoggerFactory.getLogger(getClass)

  val iterations = 50000
  var finishCnt: AtomicInteger = new AtomicInteger(0)
  var failedCnt: AtomicInteger = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {
    println("START")
    //testAkkaFutures
    testFutures
    println("END")
    var ack = 0
    while (iterations > ack) {
      ack = finishCnt.get + failedCnt.get
      println(s"$iterations - (${finishCnt.get} + ${failedCnt.get})")
      Thread.sleep(1000)
    }
  }

  def testAkkaFutures = {
    val system = ActorSystem("theSystem")
    for (itr <- 1 to iterations) {
      val future = getRandomFuture(s"f-$itr")
      //      Await.result(future, 5.second)
      future.onComplete {
        case Success(result) => {
          finishCnt.incrementAndGet
          //finishCnt += 1
          //LOG.info(s"Success: ${result._1} :: ${result._2}")
        }
        case Failure(ex) => {
          failedCnt.incrementAndGet
          //failedCnt += 1
          //LOG.error(s"Failure :: ${ex}")
        }
      }
    }
    system.shutdown
  }

  def getRandomFuture(name: String): Future[(String, String)] = Future {
    val result = 0//Random.nextInt(10) / Random.nextInt(10)
    val sleepTime = 100 + Random.nextInt(10)
    Thread.sleep(sleepTime)
    (name, s"future $name slept $sleepTime ms, result = $result")
  }

  def testFutures = {
    implicit val ec = ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(iterations))
    for (itr <- 1 to iterations) {
      val future = getRandomFuture(s"f-$itr")
      //      Await.result(future, 5.second)
      future.onComplete {
        case Success(result) => {
          finishCnt.incrementAndGet// += 1
          //LOG.info(s"Success: $fName :: ${result}")
        }
        case Failure(ex) => {
          failedCnt.incrementAndGet// += 1
          //LOG.error(s"Failure: $fName :: ${ex}")
        }
      }
    }
  }

  def testHttpRequest = {
    val httpclient = HttpAsyncClients.createDefault()
    val httpurl = "http://api.icndb.com/jokes/random" // "http://www.apache.org/"
    try {
      httpclient.start
      val request = new HttpGet(httpurl)
      val future = httpclient.execute(request, null);
      val response = future.get();
      System.out.println("Response: " + response.getStatusLine())
    } finally {
      System.out.println("Shutting down")
      httpclient.close
    }
    System.out.println("Done");

    System.exit(0)
    val url = "http://api.icndb.com/jokes/random"
    FileHelper.repeat(FileHelper.time(FileHelper.callRestApi(url), "callRestApi"), 10)
  }
}