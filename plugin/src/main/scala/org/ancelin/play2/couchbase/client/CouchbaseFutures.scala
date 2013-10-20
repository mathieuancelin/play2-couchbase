package org.ancelin.play2.couchbase.client

import net.spy.memcached.internal._
import scala.concurrent.{Promise, Future, ExecutionContext}
import com.couchbase.client.internal.{HttpCompletionListener, HttpFuture}
import net.spy.memcached.ops.OperationStatus
import play.Logger

object CouchbaseFutures {

  def waitForBulkRaw(future: BulkFuture[java.util.Map[String, AnyRef]], ec: ExecutionContext): Future[java.util.Map[String, AnyRef]] = {
    val promise = Promise[java.util.Map[String, AnyRef]]()
    future.addListener(new BulkGetCompletionListener() {
      def onComplete(f: BulkGetFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) Logger.error(f.getStatus.getMessage)
          //if (f.isDone || f.isCancelled || f.isTimeout) {
            promise.success(f.get().asInstanceOf[java.util.Map[String, AnyRef]]);
          //} else promise.failure(new Throwable(s"ListenableFuture epic fail !!! ${f.isDone} : ${f.isCancelled} : ${f.isTimeout}"))
        }
      }
    })
    promise.future
  }

  def waitForGet[T](future: GetFuture[T], ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    future.addListener(new GetCompletionListener() {
      def onComplete(f: GetFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) Logger.error(f.getStatus.getMessage)
          //if (f.isDone || f.isCancelled) {
            promise.success(f.get().asInstanceOf[T]);
          //} else promise.failure(new Throwable(s"ListenableFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
        }
      }
    })
    promise.future
  }

  def waitForHttpStatus[T](future: HttpFuture[T], ec: ExecutionContext): Future[OperationStatus] = {
    val promise = Promise[OperationStatus]()
    future.addListener(new HttpCompletionListener() {
      def onComplete(f: HttpFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) Logger.error(f.getStatus.getMessage)
          //if (f.isDone || f.isCancelled) {
            promise.success(f.getStatus);
          //} else promise.failure(new Throwable(s"ListenableFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
        }
      }
    })
    promise.future
  }

  def waitForHttp[T](future: HttpFuture[T], ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    future.addListener(new HttpCompletionListener() {
      def onComplete(f: HttpFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) Logger.error(f.getStatus.getMessage)
          //if (f.isDone || f.isCancelled) {
            promise.success(f.get().asInstanceOf[T]);
          //} else promise.failure(new Throwable(s"ListenableFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
        }
      }
    })
    promise.future
  }

  def waitForOperationStatus[T](future: OperationFuture[T], ec: ExecutionContext): Future[OperationStatus] = {
    val promise = Promise[OperationStatus]()
    future.addListener(new OperationCompletionListener() {
      def onComplete(f: OperationFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) Logger.error(f.getStatus.getMessage)
          //if (f.isDone || f.isCancelled) {
            promise.success(f.getStatus);
          //} else promise.failure(new Throwable(s"ListenableFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
        }
      }
    })
    promise.future
  }

  def waitForOperation[T](future: OperationFuture[T], ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    future.addListener(new OperationCompletionListener() {
      def onComplete(f: OperationFuture[_]) = {
        if (Constants.failWithOpStatus && (!f.getStatus.isSuccess)) {
          promise.failure(new OperationFailedException(f.getStatus))
        } else {
          if (!f.getStatus.isSuccess) Logger.error(f.getStatus.getMessage)
          //if (f.isDone || f.isCancelled) {
            promise.success(f.get().asInstanceOf[T]);
          //} else promise.failure(new Throwable(s"ListenableFuture epic fail !!! ${f.isDone} : ${f.isCancelled}"))
        }
      }
    })
    promise.future
  }
}
