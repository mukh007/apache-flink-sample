package org.mj.flink.streaming.async

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.ResultFuture
import org.mj.flink.http.akka.{AkkaHttpClient, MyHttpRespose}

import scala.collection.GenSeq
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object AsyncIOExample extends LazyLogging {
  private implicit val as: ActorSystem = ActorSystem("as")
  private implicit lazy val ec: ExecutionContext = as.dispatcher
  //ExecutionContext.fromExecutor(Executors.directExecutor())
  private implicit val mat: ActorMaterializer = ActorMaterializer()
  private lazy val httpClient = new AkkaHttpClient

  sys.addShutdownHook({
    as.terminate()
    mat.shutdown()
  })

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input = env.addSource(new SimpleSource())

    // till 4*2 is fine as 32=8*4 (parallelism) actor system max concurrent connection setting
    val asyncMapped = AsyncDataStream.orderedWait(input, 10000L, TimeUnit.MILLISECONDS, 4) {
      (input, resultFuture: ResultFuture[GenSeq[MyHttpRespose]]) => {
        val url = "http://example.org/"
        val uri = Uri(url).withQuery(Query("foo" -> "bar", "id" -> s"$input"))
        val getRequest = HttpRequest().withUri(uri).withMethod(HttpMethods.GET)
        val postRequest = HttpRequest().withUri(uri).withMethod(HttpMethods.POST).withEntity("{json:1}")

        val httpResponse = httpClient.runHttpRequests(Seq(getRequest, postRequest))
        httpResponse.onComplete({
          case Success(result) => resultFuture.complete(Iterable(result))
          case Failure(ex) =>
            logger.error(s"Failed ${ex.getMessage} ${ex.getClass}")
            resultFuture.complete(Iterable(Seq())) // Skip errors
          //resultFuture.completeExceptionally(ex)
        })
      }
    }

    asyncMapped.flatMap(_.toIterator)
      .addSink(ip => {
        logger.info(s"Sink: ${ip.responseCode}, ${ip.responseBody.length}")
      })

    env.execute("Async I/O job")
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