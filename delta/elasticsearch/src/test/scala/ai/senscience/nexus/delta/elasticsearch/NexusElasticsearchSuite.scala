package ai.senscience.nexus.delta.elasticsearch

import ai.senscience.nexus.testkit.mu.NexusSuite

import scala.concurrent.duration.{Duration, DurationInt}

abstract class NexusElasticsearchSuite extends NexusSuite {

  override def munitIOTimeout: Duration = 120.seconds

}
