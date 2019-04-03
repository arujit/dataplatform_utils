package com.goibibo.dp.utils

import java.util.{Timer, TimerTask}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.language.postfixOps

object FutureUtils {

  val timer: Timer = new Timer(true)

  /**
    * Returns the result of the provided future within the given time or a timeout exception, whichever is first
    * @param task future to execute
    * @param timeout timeout
    * @return Future[T]
    */
  def futureWithTimeout[T](task : Future[T], timeout : FiniteDuration)(implicit ec: ExecutionContext): Future[T] = {

    var p = Promise[T]


    val timerTask = new TimerTask() {
      def run() : Unit = {
        p.tryFailure(new TimeoutException())
      }
    }

    timer.schedule(timerTask, timeout.toMillis)

    task.map {
      a =>
        if(p.trySuccess(a)) {
          timerTask.cancel()
        }
    }
      .recover {
        case e: Exception =>
          if(p.tryFailure(e)) {
            timerTask.cancel()
          }
      }

    p.future
  }

}