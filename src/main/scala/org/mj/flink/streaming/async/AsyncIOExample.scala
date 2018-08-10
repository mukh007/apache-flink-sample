package org.mj.flink.streaming.async

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.ResultFuture
import org.mj.flink.http.akka.AkkaHttpClient

import scala.collection.GenSeq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

object AsyncIOExample extends LazyLogging {
  private implicit lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())
  private implicit val as: ActorSystem = ActorSystem("mj_asyncIO")
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private lazy val httpClient = new AkkaHttpClient
  private val random = new Random()
  sys.addShutdownHook({
    as.terminate()
    mat.shutdown()
  })

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input = env.addSource(new SimpleSource())

    // till 4*2 is fine as 32=8*4 (parallelism) actor system max concurrent connection setting
    val asyncMapped = AsyncDataStream.orderedWait(input, 10000L, TimeUnit.MILLISECONDS, 5) {
      (input, resultFuture: ResultFuture[GenSeq[String]]) => {
        val url = "http://example.org/"
        val uri = Uri(url).withQuery(Query("id" -> s"$input", "n" -> "m"))
        val getRequest = HttpRequest().withUri(uri).withMethod(HttpMethods.GET)
        val postRequest = HttpRequest().withUri(uri).withMethod(HttpMethods.POST).withEntity("{json:1}")

        val res = httpClient.runHttpRequests(Seq(getRequest, postRequest))
        res.onComplete({
          case Success(result) => resultFuture.complete(Iterable(result))
          case Failure(ex) => resultFuture.completeExceptionally(ex)
        })
      }
    }

    asyncMapped
      .flatMap(_.toIterator)
      .addSink(ip => {
        logger.info(s"Sink: ${ip.length}")
      })
    //    asyncMapped.addSink(ip => {
    //      println(s"Sink: ${ip.mkString("").replace("\n", " ").split(" ").head}")
    //    })

    env.execute("Async I/O job")
  }

  /**
    *
    * Needs to handle back-off retries
    * Needs to handle exceptions
    */
  private def doHttpCall(input: Int) = {
    val url = "http://example.org/"
    val getRequest = HttpRequest(uri = url)
    val result = httpClient.runHttpRequest(getRequest)
    result.map(in => (input, in))
  }

  /**
    * Needs to handle exceptions
    */
  private def doStuff(input: Int): Future[String] = {
    Future {
      s"$input-MJ-${input / (0 + random.nextInt(10))}-${System.currentTimeMillis}"
    }
  }
}

class SimpleSource extends ParallelSourceFunction[Int] {
  var running = true
  var counter = 0

  override def run(ctx: SourceContext[Int]): Unit = {
    while (running) {
      ctx.getCheckpointLock.synchronized {
        ctx.collect(counter)
      }
      counter += 1

      Thread.sleep(10L)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}