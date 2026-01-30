package ai.senscience.nexus.testkit.scalatest.ce

import cats.effect.IO
import cats.effect.unsafe.implicits.*
import org.scalatest.Assertions.*
import org.scalatest.{Assertion, AsyncTestSuite}

import scala.concurrent.{ExecutionContext, Future}

trait CatsEffectAsyncScalaTestAdapter extends CatsEffectAsyncScalaTestAdapterLowPrio {

  self: AsyncTestSuite =>

  given ioToFutureAssertion: Conversion[IO[Assertion], Future[Assertion]] = _.unsafeToFuture()

  given futureListToFutureAssertion: Conversion[Future[List[Assertion]], Future[Assertion]] =
    _.map(_ => succeed)
}

trait CatsEffectAsyncScalaTestAdapterLowPrio {

  given ioListToFutureAssertion: ExecutionContext => Conversion[IO[List[Assertion]], Future[Assertion]] =
    _.unsafeToFuture().map(_ => succeed)
}
