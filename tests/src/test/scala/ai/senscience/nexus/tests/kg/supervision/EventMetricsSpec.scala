package ai.senscience.nexus.tests.kg.supervision

import ai.senscience.nexus.tests.BaseIntegrationSpec
import ai.senscience.nexus.tests.Identity.{Anonymous, ServiceAccount}
import ai.senscience.nexus.tests.iam.types.Permission.Supervision
import io.circe.Json

class EventMetricsSpec extends BaseIntegrationSpec {

  private val permission = Supervision.Read.value

  "The event metrics statistics endpoint" should {
    val endpoint = "/event-metrics/statistics"

    s"reject calls without $permission permission" in {
      deltaClient.get[Json](endpoint, Anonymous) { expectForbidden }
    }

    s"accept calls with $permission" in {
      deltaClient.get[Json](endpoint, ServiceAccount) { expectOk }
    }
  }

  "The event metrics indexing failures endpoint" should {

    val endpoint = "/event-metrics/failures"

    s"reject calls without $permission permission" in {
      deltaClient.get[Json](endpoint, Anonymous) { expectForbidden }
    }

    s"accept calls with $permission" in {
      deltaClient.get[Json](endpoint, ServiceAccount) { expectOk }
    }
  }

  "The event metrics offset endpoint" should {

    val endpoint = "/event-metrics/offset"

    s"reject reading offset without $permission permission" in {
      deltaClient.get[Json](endpoint, Anonymous) { expectForbidden }
    }

    s"accept reading with $permission" in {
      deltaClient.get[Json](endpoint, ServiceAccount) { expectOk }
    }

    s"reject restart indexing without $permission permission" in {
      deltaClient.delete[Json](endpoint, Anonymous) { expectForbidden }
    }

    s"accept restart indexin with $permission" in {
      deltaClient.delete[Json](endpoint, ServiceAccount) { expectOk }
    }
  }

}
