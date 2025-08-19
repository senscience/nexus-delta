package ai.senscience.nexus.tests.kg.supervision

import ai.senscience.nexus.tests.BaseIntegrationSpec
import ai.senscience.nexus.tests.Identity.{Anonymous, ServiceAccount}
import ai.senscience.nexus.tests.iam.types.Permission.Supervision
import io.circe.Json

class IndexingSupervisionSpec extends BaseIntegrationSpec {

  "The indexing error count endpoint" should {

    val endpoint = "/supervision/indexing/errors/count"

    s"reject calls without ${Supervision.Read.value} permission" in {
      deltaClient.get[Json](endpoint, Anonymous) { expectForbidden }
    }

    s"accept calls with ${Supervision.Read.value}" in {
      deltaClient.get[Json](endpoint, ServiceAccount) { expectOk }
    }
  }

  "The indexing error latest endpoint" should {

    val endpoint = "/supervision/indexing/errors/latest"

    s"reject calls without ${Supervision.Read.value} permission" in {
      deltaClient.get[Json](endpoint, Anonymous) { expectForbidden }
    }

    s"accept calls with ${Supervision.Read.value}" in {
      deltaClient.get[Json](endpoint, ServiceAccount) { expectOk }
    }
  }

}
