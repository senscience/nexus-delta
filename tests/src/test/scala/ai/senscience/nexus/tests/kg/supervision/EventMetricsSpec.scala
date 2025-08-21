package ai.senscience.nexus.tests.kg.supervision

import ai.senscience.nexus.tests.BaseIntegrationSpec
import ai.senscience.nexus.tests.Identity.{Anonymous, ServiceAccount}
import ai.senscience.nexus.tests.iam.types.Permission.Supervision
import io.circe.Json

class EventMetricsSpec extends BaseIntegrationSpec {

  "The event metrics statistics endpoint" should {

    val endpoint = "/event-metrics/statistics"

    s"reject calls without ${Supervision.Read.value} permission" in {
      deltaClient.get[Json](endpoint, Anonymous) { expectForbidden }
    }

    s"accept calls with ${Supervision.Read.value}" in {
      deltaClient.get[Json](endpoint, ServiceAccount) { expectOk }
    }
  }

  "The event metrics indexing failures endpoint" should {

    val endpoint = "/event-metrics/failures"

    s"reject calls without ${Supervision.Read.value} permission" in {
      deltaClient.get[Json](endpoint, Anonymous) { expectForbidden }
    }

    s"accept calls with ${Supervision.Read.value}" in {
      deltaClient.get[Json](endpoint, ServiceAccount) { expectOk }
    }
  }

}
