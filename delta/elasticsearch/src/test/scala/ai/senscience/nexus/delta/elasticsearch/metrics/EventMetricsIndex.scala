package ai.senscience.nexus.delta.elasticsearch.metrics

import ai.senscience.nexus.delta.kernel.utils.ClasspathResourceLoader
import cats.effect.kernel.Resource
import munit.CatsEffectSuite
import munit.catseffect.IOFixture

object EventMetricsIndex {

  private given ClasspathResourceLoader = ClasspathResourceLoader()

  trait Fixture { self: CatsEffectSuite =>
    val metricsIndex: IOFixture[MetricsIndexDef] =
      ResourceSuiteLocalFixture("metrics-index", Resource.eval(MetricsIndexDef("test")))
  }

}
