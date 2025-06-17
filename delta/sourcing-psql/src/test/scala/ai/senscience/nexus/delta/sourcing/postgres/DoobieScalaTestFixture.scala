package ai.senscience.nexus.delta.sourcing.postgres

import ai.senscience.nexus.delta.sourcing.partition.{DatabasePartitioner, PartitionStrategy}
import ai.senscience.nexus.delta.sourcing.{DDLLoader, Transactors}
import ai.senscience.nexus.testkit.Generators
import ai.senscience.nexus.testkit.clock.FixedClock
import ai.senscience.nexus.testkit.scalatest.ce.CatsIOValues
import cats.effect.IO
import org.scalatest.{BeforeAndAfterAll, Suite}

trait DoobieScalaTestFixture
    extends BeforeAndAfterAll
    with PostgresDocker
    with Generators
    with CatsIOValues
    with FixedClock {

  self: Suite =>

  var xas: Transactors              = _
  private var xasTeardown: IO[Unit] = _

  private val defaultPartitioningStrategy = PartitionStrategy.Hash(1)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val (x, t) = Transactors
      .test(container.getHost, container.getMappedPort(5432), "postgres", "postgres", "postgres")
      .evalTap(DDLLoader.dropAndCreateDDLs(defaultPartitioningStrategy, _))
      .evalTap(DatabasePartitioner(defaultPartitioningStrategy, _))
      .allocated
      .accepted
    xas = x
    xasTeardown = t
  }

  override def afterAll(): Unit = {
    xasTeardown.accepted
    super.afterAll()
  }

}
