package ai.senscience.nexus.delta.sdk.stream

import ai.senscience.nexus.delta.kernel.utils.IOFuture
import cats.effect.*
import cats.effect.kernel.Resource.ExitCase
import cats.effect.unsafe.implicits.*
import fs2.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.{Sink as PekkoSink, Source as PekkoSource, *}

/**
  * Converts a fs2 stream to an Pekko source Original code from the streamz library from Martin Krasser (published under
  * Apache License 2.0):
  * https://github.com/krasserm/streamz/blob/master/streamz-converter/src/main/scala/streamz/converter/Converter.scala
  */
object StreamConverter {

  private def publisherStream[A](publisher: SourceQueueWithComplete[A], stream: Stream[IO, A]): Stream[IO, Unit] = {
    def publish(a: A): IO[Option[Unit]] =
      IOFuture
        .defaultCancelable(IO.delay(publisher.offer(a)))
        .flatMap {
          case QueueOfferResult.Enqueued       => IO.pure(Some(()))
          case QueueOfferResult.Failure(cause) => IO.raiseError[Option[Unit]](cause)
          case QueueOfferResult.QueueClosed    => IO.none
          case QueueOfferResult.Dropped        =>
            IO.raiseError[Option[Unit]](
              new IllegalStateException("This should never happen because we use OverflowStrategy.backpressure")
            )
        }
        .recover {
          // This handles a race condition between `interruptWhen` and `publish`.
          // There's no guarantee that, when the pekko sink is terminated, we will observe the
          // `interruptWhen` termination before calling publish one last time.
          // Such a call fails with StreamDetachedException
          case _: StreamDetachedException => None
        }

    def watchCompletion: IO[Unit]    = IOFuture.defaultCancelable(IO.delay(publisher.watchCompletion())).void
    def fail(e: Throwable): IO[Unit] = IO.delay(publisher.fail(e)) >> watchCompletion
    def complete: IO[Unit]           = IO.delay(publisher.complete()) >> watchCompletion

    stream
      .interruptWhen(watchCompletion.attempt)
      .evalMap(publish)
      .unNoneTerminate
      .onFinalizeCase {
        case ExitCase.Succeeded | ExitCase.Canceled => complete
        case ExitCase.Errored(e)                    => fail(e)
      }
  }

  def apply[A](stream: Stream[IO, A]): PekkoSource[A, Any] = {
    val source = PekkoSource.queue[A](0, OverflowStrategy.backpressure)
    // A sink that runs an FS2 publisherStream when consuming the publisher actor (= materialized value) of source
    val sink   = PekkoSink.foreach[SourceQueueWithComplete[A]] { p =>
      // Fire and forget Future so it runs in the background
      publisherStream[A](p, stream).compile.drain.unsafeToFuture()
      ()
    }

    PekkoSource
      .fromGraph(GraphDSL.createGraph(source) { implicit builder => source =>
        import GraphDSL.Implicits.*
        builder.materializedValue ~> sink
        SourceShape(source.out)
      })
      .mapMaterializedValue(_ => NotUsed)
  }

  def apply[A](
      source: Graph[SourceShape[A], NotUsed]
  )(implicit materializer: Materializer): Stream[IO, A] =
    Stream.force {
      IO.delay {
        val subscriber = PekkoSource.fromGraph(source).toMat(PekkoSink.queue[A]())(Keep.right).run()
        subscriberStream[A](subscriber)
      }
    }

  private def subscriberStream[A](
      subscriber: SinkQueueWithCancel[A]
  ): Stream[IO, A] = {
    val pull   = IOFuture.defaultCancelable(IO.delay(subscriber.pull()))
    val cancel = IO.delay(subscriber.cancel())
    Stream.repeatEval(pull).unNoneTerminate.onFinalize(cancel)
  }

}
